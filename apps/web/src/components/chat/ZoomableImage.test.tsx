// @vitest-environment jsdom
import { act, createRef, type KeyboardEventHandler } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { ZoomableImage, type ZoomableImageHandle } from "./ZoomableImage";

describe("image zoom interactions", () => {
  let gallery = vi.fn<KeyboardEventHandler<HTMLDivElement>>();
  let root: Root;
  let container: HTMLDivElement;
  let resize: (entries: { contentRect: { width: number; height: number } }[]) => void;
  const handle = createRef<ZoomableImageHandle>();
  const region = () => container.querySelector<HTMLElement>('[role="region"]')!;
  const input = () => container.querySelector<HTMLInputElement>("input")!;
  const percent = () => container.querySelector('[aria-live="polite"]')!.textContent;
  const image = () => container.querySelector<HTMLImageElement>("img")!;
  const dispatch = async (target: Element, event: Event) =>
    act(() => {
      target.dispatchEvent(event);
    });
  const click = async (target: Element, detail = 1) =>
    dispatch(target, new MouseEvent("click", { bubbles: true, detail, clientX: 100, clientY: 50 }));
  const key = async (target: Element, key: string) =>
    dispatch(target, new KeyboardEvent("keydown", { bubbles: true, key }));
  const edit = async (value: string, commit = "Enter") => {
    await act(() => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(
        input(),
        value,
      );
      input().dispatchEvent(new Event("input", { bubbles: true }));
    });
    if (commit === "blur") await dispatch(input(), new FocusEvent("focusout", { bubbles: true }));
    else await key(input(), commit);
  };
  const render = async (layout: "dialog" | "panel" = "dialog", src = "fixture.png") => {
    await act(() =>
      root.render(
        <div onKeyDown={gallery}>
          <ZoomableImage
            key={src}
            ref={handle}
            src={src}
            name="Fixture"
            layout={layout}
            onError={() => {}}
          />
        </div>,
      ),
    );
    Object.defineProperties(region(), {
      clientWidth: { configurable: true, value: 400 },
      clientHeight: { configurable: true, value: 200 },
      offsetWidth: { configurable: true, value: 400 },
      scrollWidth: { configurable: true, value: 800 },
    });
    Object.defineProperties(image(), {
      naturalWidth: { value: 1000 },
      naturalHeight: { value: 500 },
    });
    await dispatch(image(), new Event("load"));
  };

  beforeEach(() => {
    vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
    vi.stubGlobal(
      "ResizeObserver",
      class {
        constructor(callback: typeof resize) {
          resize = callback;
        }
        observe() {}
        disconnect() {}
      },
    );
    gallery = vi.fn<KeyboardEventHandler<HTMLDivElement>>();
    container = document.createElement("div");
    document.body.append(container);
    root = createRoot(container);
  });
  afterEach(async () => {
    await act(() => root.unmount());
    container.remove();
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("counts rapid clicks, changes cursor, cycles with Enter and space, and resets on a new image", async () => {
    await render("panel");
    for (const [index, percent] of [110, 120, 130, 140, 150, 100].entries()) {
      await click(image(), index + 1);
      expect(container.querySelector('[aria-live="polite"]')!.textContent).toBe(`${percent}% zoom`);
      expect(region().style.cursor).toBe(percent >= 150 ? "zoom-out" : "zoom-in");
    }
    await key(region(), "Enter");
    await key(region(), " ");
    expect(input().value).toBe("120");
    await render("dialog", "next.png");
    expect(percent()).toBe("100% zoom");
    expect(input()).toBeNull();
  });

  it("commits valid percentages, restores invalid entries, steps by ten points and resets", async () => {
    await render("panel");
    await edit("245");
    expect(input().value).toBe("245");
    await edit("60", "blur");
    expect(input().value).toBe("60");
    for (const value of ["", "bad", "59", "801"]) {
      await edit(value);
      expect(input().value).toBe("60");
    }
    await click(container.querySelector('[aria-label="Zoom out"]')!);
    expect(input().value).toBe("60");
    await click(container.querySelector('[aria-label="Zoom in"]')!);
    expect(input().value).toBe("70");
    await key(region(), "+");
    expect(input().value).toBe("80");
    await key(region(), "-");
    expect(input().value).toBe("70");
    await key(region(), "0");
    expect(input().value).toBe("100");
    await edit("800");
    await click(container.querySelector('[aria-label="Zoom in"]')!);
    expect(input().value).toBe("800");
    await click(container.querySelector('[aria-label="Reset zoom to 100%"]')!);
    expect(input().value).toBe("100");
    gallery.mockClear();
    await key(input(), "ArrowRight");
    expect(gallery).not.toHaveBeenCalled();
  });

  it("hides dialog controls while wheel zoom stops at sixty percent and can zoom back in", async () => {
    await render("dialog");
    const fittedWidth = Number.parseFloat(image().style.width);
    expect(container.querySelector('[role="toolbar"]')).toBeNull();
    await dispatch(
      region(),
      new WheelEvent("wheel", {
        bubbles: true,
        cancelable: true,
        deltaY: 10000,
        clientX: 100,
        clientY: 50,
      }),
    );
    expect(percent()).toBe("60% zoom");
    expect(Number.parseFloat(image().style.width)).toBeCloseTo(fittedWidth * 0.6);
    await dispatch(
      region(),
      new WheelEvent("wheel", {
        bubbles: true,
        cancelable: true,
        deltaY: 10000,
      }),
    );
    expect(percent()).toBe("60% zoom");
    await dispatch(
      region(),
      new WheelEvent("wheel", {
        bubbles: true,
        cancelable: true,
        deltaY: -100,
      }),
    );
    expect(Number.parseFloat(image().style.width)).toBeGreaterThan(fittedWidth * 0.6);
    await key(region(), "0");
    expect(percent()).toBe("100% zoom");
  });

  it("fits the constrained panel, refits on resize, and keeps pointer-anchored wheel zoom and keyboard pan", async () => {
    await render("panel");
    await act(() => resize([{ contentRect: { width: 400, height: 200 } }]));
    expect(image().style.width).toBe("400px");
    expect(image().style.height).toBe("200px");
    await click(image());
    expect(Number.parseFloat(image().style.width)).toBeCloseTo(440);
    expect(region().scrollLeft).toBeCloseTo(10);
    await dispatch(
      region(),
      new WheelEvent("wheel", {
        bubbles: true,
        cancelable: true,
        deltaY: -100,
        clientX: 100,
        clientY: 50,
      }),
    );
    expect(Number(input().value)).toBeGreaterThan(110);
    const before = region().scrollLeft;
    expect(handle.current!.pan("ArrowRight")).toBe(true);
    expect(region().scrollLeft).toBe(before + 40);
    await act(() => resize([{ contentRect: { width: 200, height: 120 } }]));
    expect(input().value).toBe("100");
    expect(image().style.width).toBe("200px");
    expect(image().style.height).toBe("100px");
  });

  it("leaves gallery arrows available without horizontal overflow and pans only while zoomed", async () => {
    await render("dialog");
    await key(region(), "Enter");
    Object.defineProperty(region(), "scrollWidth", { configurable: true, value: 400 });
    const left = region().scrollLeft;
    expect(handle.current!.pan("ArrowRight")).toBe(false);
    expect(region().scrollLeft).toBe(left);
    const top = region().scrollTop;
    expect(handle.current!.pan("ArrowDown")).toBe(true);
    expect(region().scrollTop).toBe(top + 40);
    Object.defineProperty(region(), "scrollWidth", { configurable: true, value: 800 });
    expect(handle.current!.pan("ArrowRight")).toBe(true);
    expect(region().scrollLeft).toBe(left + 40);
    await key(region(), "0");
    expect(handle.current!.pan("ArrowDown")).toBe(false);
  });

  it("suppresses the release click after a drag and shows grabbing only after movement", async () => {
    await render("panel");
    await edit("150");
    region().setPointerCapture = vi.fn();
    region().hasPointerCapture = () => true;
    region().releasePointerCapture = vi.fn();
    const pointer = (type: string, x: number) => {
      const event = new MouseEvent(type, { bubbles: true, button: 0, clientX: x, clientY: 50 });
      Object.defineProperties(event, { pointerId: { value: 1 }, pointerType: { value: "mouse" } });
      return event;
    };
    await dispatch(region(), pointer("pointerdown", 100));
    expect(region().style.cursor).toBe("zoom-out");
    const before = region().scrollLeft;
    await dispatch(region(), pointer("pointermove", 80));
    expect(region().style.cursor).toBe("grabbing");
    expect(region().scrollLeft).toBe(before + 20);
    await dispatch(region(), pointer("pointerup", 80));
    await click(image());
    expect(input().value).toBe("150");
    expect(region().style.cursor).toBe("zoom-out");
    await dispatch(region(), pointer("pointerdown", 100));
    await dispatch(region(), pointer("pointerup", 100));
    await click(image());
    expect(input().value).toBe("100");
  });
});

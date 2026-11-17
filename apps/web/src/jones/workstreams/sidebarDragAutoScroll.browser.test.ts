import { describe, expect, it, vi } from "vite-plus/test";
import { createSidebarDragAutoScroll } from "./sidebarDragAutoScroll";

function fixture() {
  const viewport = document.createElement("div");
  viewport.dataset.slot = "scroll-area-viewport";
  viewport.style.cssText = "position:fixed;top:100px;left:0;width:240px;height:300px;overflow:auto";
  const source = document.createElement("div");
  source.style.height = "2000px";
  viewport.append(source);
  document.body.append(viewport);
  viewport.scrollTop = 500;
  const onScroll = vi.fn();
  const scroll = createSidebarDragAutoScroll(source, onScroll);
  return {
    viewport,
    onScroll,
    scroll,
    [Symbol.dispose]() {
      scroll.dispose();
      viewport.remove();
    },
  };
}

describe("sidebar drag edge scrolling", () => {
  it("keeps scrolling beneath a stationary pointer and refreshes the insertion target", async () => {
    using f = fixture();
    const point = { x: 120, y: 105 };
    f.scroll.move(point);
    await vi.waitFor(() => expect(f.viewport.scrollTop).toBeLessThan(450));
    expect(f.onScroll).toHaveBeenCalledWith(point);
    const top = f.viewport.scrollTop;
    await vi.waitFor(() => expect(f.viewport.scrollTop).toBeLessThan(top - 40));
    f.scroll.move({ x: 120, y: 395 });
    const bottomStart = f.viewport.scrollTop;
    await vi.waitFor(() => expect(f.viewport.scrollTop).toBeGreaterThan(bottomStart + 40));
  });

  it.each([
    ["center", { x: 120, y: 250 }],
    ["composer", { x: 400, y: 105 }],
  ])("stops immediately when the pointer moves to the %s", async (_name, point) => {
    using f = fixture();
    f.scroll.move({ x: 120, y: 105 });
    await vi.waitFor(() => expect(f.viewport.scrollTop).toBeLessThan(480));
    f.scroll.move(point);
    const top = f.viewport.scrollTop;
    await new Promise<void>((resolve) =>
      requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
    );
    expect(f.viewport.scrollTop).toBe(top);
  });

  it("cancels its pending frame on release or cancellation", async () => {
    using f = fixture();
    f.scroll.move({ x: 120, y: 105 });
    f.scroll.dispose();
    await new Promise<void>((resolve) =>
      requestAnimationFrame(() => requestAnimationFrame(() => resolve())),
    );
    expect(f.viewport.scrollTop).toBe(500);
    expect(f.onScroll).not.toHaveBeenCalled();
  });
});

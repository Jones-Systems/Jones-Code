import { describe, expect, it, vi } from "vite-plus/test";
import { fakeTimers } from "../../../test/render";
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
  it("keeps scrolling beneath a stationary pointer and refreshes the insertion target", () => {
    using _timers = fakeTimers();
    using f = fixture();
    const point = { x: 120, y: 105 };
    f.scroll.move(point);
    vi.advanceTimersByTime(200);
    expect(f.viewport.scrollTop).toBeLessThan(450);
    expect(f.onScroll).toHaveBeenCalledWith(point);
    const top = f.viewport.scrollTop;
    vi.advanceTimersByTime(200);
    expect(f.viewport.scrollTop).toBeLessThan(top - 40);
    f.scroll.move({ x: 120, y: 395 });
    const bottomStart = f.viewport.scrollTop;
    vi.advanceTimersByTime(200);
    expect(f.viewport.scrollTop).toBeGreaterThan(bottomStart + 40);
  });

  it.each([
    { name: "center", point: { x: 120, y: 250 } },
    { name: "composer", point: { x: 400, y: 105 } },
  ])("stops immediately when the pointer moves to the $name", ({ point }) => {
    using _timers = fakeTimers();
    using f = fixture();
    f.scroll.move({ x: 120, y: 105 });
    vi.advanceTimersByTime(200);
    expect(f.viewport.scrollTop).toBeLessThan(480);
    f.scroll.move(point);
    const top = f.viewport.scrollTop;
    vi.advanceTimersByTime(200);
    expect(f.viewport.scrollTop).toBe(top);
  });

  it("cancels its pending frame on release or cancellation", () => {
    using _timers = fakeTimers();
    using f = fixture();
    f.scroll.move({ x: 120, y: 105 });
    f.scroll.dispose();
    vi.advanceTimersByTime(200);
    expect(f.viewport.scrollTop).toBe(500);
    expect(f.onScroll).not.toHaveBeenCalled();
  });
});

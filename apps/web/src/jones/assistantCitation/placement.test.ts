import { describe, expect, it } from "vite-plus/test";

import { resolveCitationPlacement, type CitationPoint, type CitationRect } from "./placement";

const toolbar = { width: 64, height: 24 };
const viewport = { width: 800, height: 600 };
const single = [{ left: 200, right: 400, top: 200, bottom: 220 }];
const place = (rects: CitationRect[], pointer: CitationPoint | null) =>
  resolveCitationPlacement({ rects, pointer, toolbar, viewport });

describe("citation placement", () => {
  it.each([
    [
      { x: 200, y: 210 },
      { x: 128, y: 198 },
    ],
    [
      { x: 400, y: 210 },
      { x: 408, y: 198 },
    ],
    [
      { x: 300, y: 200 },
      { x: 268, y: 168 },
    ],
    [
      { x: 300, y: 220 },
      { x: 268, y: 228 },
    ],
  ])("attaches outside the text edge nearest release %j", (pointer, expected) => {
    expect(place(single, pointer)).toEqual(expected);
  });

  it("uses the start line for a reverse multiline selection", () => {
    const rects = [...single, { left: 200, right: 300, top: 220, bottom: 240 }];
    expect(place(rects, { x: 201, y: 207 })).toEqual({ x: 128, y: 195 });
  });

  it("uses the final line edge rather than the multiline bounding box", () => {
    const rects = [...single, { left: 200, right: 300, top: 220, bottom: 240 }];
    expect(place(rects, { x: 300, y: 238 })).toEqual({ x: 308, y: 226 });
  });

  it("keeps a shorter line's toolbar from covering another selected line", () => {
    const rects = [
      { left: 200, right: 300, top: 200, bottom: 220 },
      { left: 200, right: 400, top: 220, bottom: 240 },
    ];
    expect(place(rects, { x: 300, y: 219 })).toEqual({ x: 268, y: 168 });
  });

  it("does not open underneath a mouse released outside the text", () => {
    const pointer = { x: 430, y: 210 };
    const result = place(single, pointer)!;
    expect(result).toEqual({ x: 368, y: 168 });
    expect(pointer.y).toBeGreaterThan(result.y + toolbar.height);
  });

  it("anchors keyboard selection below its final text line", () => {
    expect(place(single, null)).toEqual({ x: 368, y: 228 });
  });

  it("chooses a fitting side at the browser's right edge", () => {
    expect(place([{ left: 500, right: 795, top: 200, bottom: 220 }], { x: 795, y: 210 })).toEqual({
      x: 728,
      y: 168,
    });
  });

  it("clips offscreen rects and keeps the full toolbar inside the window", () => {
    expect(place([{ left: -50, right: 300, top: -20, bottom: 20 }], { x: 0, y: 0 })).toEqual({
      x: 8,
      y: 28,
    });
  });

  it("hides when no visible text or no outside space remains", () => {
    expect(place([{ left: 200, right: 400, top: -50, bottom: -20 }], null)).toBeNull();
    expect(place([{ left: 0, right: 800, top: 0, bottom: 600 }], null)).toBeNull();
  });
});

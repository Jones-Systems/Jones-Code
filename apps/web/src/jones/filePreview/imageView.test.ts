import { expect, it } from "vite-plus/test";
import { imageFit } from "./imageView";

it("fits panels independently of the window and preserves SVG upscaling", () => {
  const input = {
    naturalSize: { width: 100, height: 50 },
    viewerSize: { width: 1280, height: 800 },
    panelSize: { width: 400, height: 200 },
    layout: "panel" as const,
    maxHeight: 640,
    mimeType: undefined,
  };
  expect(imageFit(input)).toBe(1);
  expect(imageFit({ ...input, mimeType: "image/svg+xml" })).toBe(4);
  expect(imageFit({ ...input, panelSize: { width: 40, height: 20 } })).toBe(0.4);
  expect(imageFit({ ...input, panelSize: { width: 0, height: 0 } })).toBe(0);
  expect(
    imageFit({
      ...input,
      layout: "dialog",
      viewerSize: { width: 100, height: 100 },
      maxHeight: 50,
    }),
  ).toBe(0.92);
});

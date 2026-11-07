export interface ImageView {
  readonly width: number;
  readonly height: number;
  readonly zoom: number;
  readonly fit: number;
}

export function imageFit({
  naturalSize,
  viewerSize,
  panelSize,
  layout,
  mimeType,
  maxHeight,
}: {
  readonly naturalSize: { readonly width: number; readonly height: number };
  readonly viewerSize: { readonly width: number; readonly height: number };
  readonly panelSize: { readonly width: number; readonly height: number };
  readonly layout: "dialog" | "panel";
  readonly mimeType: string | undefined;
  readonly maxHeight: number;
}): number {
  // Preserve lossless SVG upscaling while respecting the actual panel bounds.
  return Math.max(
    0,
    Math.min(
      mimeType === "image/svg+xml" ? Infinity : 1,
      (layout === "panel"
        ? panelSize.width
        : viewerSize.width * 0.92 - (viewerSize.width >= 640 ? 96 : 0)) / (naturalSize.width || 1),
      (layout === "panel" ? panelSize.height : maxHeight) / (naturalSize.height || 1),
    ),
  );
}

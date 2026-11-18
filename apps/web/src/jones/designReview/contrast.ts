/**
 * WCAG contrast of rendered text against the backgrounds painted behind it. The browser resolves
 * every color first (color-mix, light-dark, theme variables), and a 1px canvas composites the
 * translucent layers, so the ratio describes what the product actually paints.
 */

let canvasContext: CanvasRenderingContext2D | null | undefined;

function paintContext(): CanvasRenderingContext2D | null {
  if (canvasContext === undefined) {
    const canvas = document.createElement("canvas");
    canvas.width = 1;
    canvas.height = 1;
    canvasContext = canvas.getContext("2d", { willReadFrequently: true });
  }
  return canvasContext;
}

/** A canvas ignores a color it cannot parse, so the fill keeps whichever sentinel preceded it. */
function parsesAsFill(context: CanvasRenderingContext2D, color: string): boolean {
  context.fillStyle = "#010203";
  context.fillStyle = color;
  const first = context.fillStyle;
  context.fillStyle = "#040506";
  context.fillStyle = color;
  return first !== "#010203" || context.fillStyle !== "#040506";
}

/** Paints each color over the last and returns the pixel, or null if a color does not parse. */
function composite(context: CanvasRenderingContext2D, colors: ReadonlyArray<string>) {
  context.globalCompositeOperation = "source-over";
  context.fillStyle = "#ffffff";
  context.fillRect(0, 0, 1, 1);
  for (const color of colors) {
    if (!parsesAsFill(context, color)) return null;
    context.fillRect(0, 0, 1, 1);
  }
  const [red = 0, green = 0, blue = 0] = context.getImageData(0, 0, 1, 1).data;
  return [red, green, blue] as const;
}

function relativeLuminance([red, green, blue]: readonly [number, number, number]): number {
  const linear = (channel: number) => {
    const value = channel / 255;
    return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
  };
  return 0.2126 * linear(red) + 0.7152 * linear(green) + 0.0722 * linear(blue);
}

/** Background colors from the root down to the text's own element. */
function backgroundStack(text: Element): string[] {
  const layers: string[] = [];
  for (let node: Element | null = text; node !== null; node = node.parentElement) {
    layers.push(getComputedStyle(node).backgroundColor);
  }
  return layers.reverse();
}

export function measureTextContrast(text: Element): number | null {
  const context = paintContext();
  if (context === null) return null;
  const backgrounds = backgroundStack(text);
  const background = composite(context, backgrounds);
  const foreground = composite(context, [...backgrounds, getComputedStyle(text).color]);
  if (background === null || foreground === null) return null;
  const [lighter, darker] = [relativeLuminance(foreground), relativeLuminance(background)].sort(
    (a, b) => b - a,
  );
  return (lighter! + 0.05) / (darker! + 0.05);
}

/**
 * Measures the same text at each value. `apply` restyles the rendered product for one value and
 * `restore` puts it back; both run inside this call, so the page never paints an applied value.
 */
export function measureTextContrastAcross<Value>(
  text: Element,
  values: ReadonlyArray<Value>,
  apply: (value: Value) => void,
  restore: () => void,
): ReadonlyArray<{ value: Value; ratio: number | null }> {
  try {
    return values.map((value) => {
      apply(value);
      return { value, ratio: measureTextContrast(text) };
    });
  } finally {
    restore();
  }
}

export function formatContrast(ratio: number | null): string {
  return ratio === null ? "unavailable" : `${ratio.toFixed(1)}:1`;
}

/** WCAG 2 levels for body-size text, which a sidebar title is. */
export function contrastLevel(ratio: number | null): string {
  if (ratio === null) return "";
  if (ratio >= 7) return "AAA";
  if (ratio >= 4.5) return "AA";
  return "below AA";
}

import { BrainIcon, HouseIcon, ServerIcon } from "lucide-react";
import { expect, it } from "vite-plus/test";
import { render } from "../../../test/render";
import { EnvironmentMachineIcon } from "../../components/EnvironmentMachineIcon";
import { SidebarAppearanceOverride } from "../sidebarAppearance/SidebarAppearanceContext";
import {
  DEFAULT_SIDEBAR_APPEARANCE,
  ORIGINAL_SIDEBAR_APPEARANCE,
  type SidebarAppearance,
} from "../sidebarAppearance/model";
import { PersonalMachineSymbol } from "./PersonalMachineSymbol";

const variants = ["outline", "left", "right", "filled"] as const;

function symbol(label: string, kind: "server" | "laptop" = "server") {
  return (
    <PersonalMachineSymbol
      label={label}
      kind={kind}
      fallback={ServerIcon}
      size={12}
      width={12}
      height={12}
      className="size-3"
    />
  );
}

function rootPaths(svg: SVGSVGElement) {
  return Array.from(svg.querySelectorAll<SVGPathElement>(":scope > path"));
}

function svgAt(container: HTMLElement, index: number) {
  const svg = container.querySelectorAll<SVGSVGElement>("svg")[index]!;
  expect(svg.getBoundingClientRect().width).toBe(12);
  expect(svg.getBoundingClientRect().height).toBe(12);
  return svg;
}

async function raster(svg: SVGSVGElement, withoutCuts = false) {
  const clone = svg.cloneNode(true) as SVGSVGElement;
  if (withoutCuts) {
    for (const path of rootPaths(clone)) path.removeAttribute("mask");
  }
  clone.style.color = getComputedStyle(svg).color;
  clone.style.width = "12px";
  clone.style.height = "12px";
  const url = URL.createObjectURL(
    new Blob([new XMLSerializer().serializeToString(clone)], { type: "image/svg+xml" }),
  );
  try {
    const image = new Image();
    image.src = url;
    await image.decode();
    const canvas = document.createElement("canvas");
    canvas.width = canvas.height = 12;
    const context = canvas.getContext("2d")!;
    context.drawImage(image, 0, 0, 12, 12);
    return context.getImageData(0, 0, 12, 12);
  } finally {
    URL.revokeObjectURL(url);
  }
}

function halfInk(pixels: ImageData, side: "left" | "right") {
  let alpha = 0;
  for (let y = 0; y < 12; y++) {
    for (let x = side === "left" ? 0 : 6; x < (side === "left" ? 6 : 12); x++) {
      alpha += pixels.data[(y * 12 + x) * 4 + 3]!;
    }
  }
  return alpha;
}

it("keeps the Lucide VPS outline outside the sidebar and in both presets", async () => {
  await using screen = await render(
    <>
      {symbol("Laptop")}
      <HouseIcon size={12} className="size-3" />
      {symbol("VPS")}
      <BrainIcon size={12} className="size-3" />
      <SidebarAppearanceOverride value={ORIGINAL_SIDEBAR_APPEARANCE}>
        {symbol("Laptop")}
        {symbol("VPS")}
      </SidebarAppearanceOverride>
      <SidebarAppearanceOverride value={DEFAULT_SIDEBAR_APPEARANCE}>
        {symbol("Laptop")}
        {symbol("VPS")}
      </SidebarAppearanceOverride>
      {symbol("VPS")}
    </>,
    { router: false },
  );
  const house = svgAt(screen.container, 1).outerHTML;
  const brain = svgAt(screen.container, 3).outerHTML;
  expect(svgAt(screen.container, 0).outerHTML).toBe(house);
  expect(svgAt(screen.container, 2).outerHTML).toBe(brain);
  expect(svgAt(screen.container, 4).outerHTML).toBe(house);
  expect(svgAt(screen.container, 5).outerHTML).toBe(brain);
  expect(svgAt(screen.container, 6).querySelector("mask")).not.toBeNull();
  expect(svgAt(screen.container, 7).outerHTML).toBe(brain);
  expect(svgAt(screen.container, 8).outerHTML).toBe(brain);
});

it("renders the approved hemispheres with only the chosen halves filled and cut", async () => {
  await using screen = await render(
    <>
      {variants.map((vpsSymbol) => (
        <SidebarAppearanceOverride
          key={vpsSymbol}
          value={{ ...DEFAULT_SIDEBAR_APPEARANCE, vpsSymbol }}
        >
          {symbol("VPS")}
        </SidebarAppearanceOverride>
      ))}
    </>,
    { router: false },
  );
  for (const [index, variant] of variants.entries()) {
    const svg = svgAt(screen.container, index);
    if (variant === "outline") {
      expect(svg.classList.contains("lucide-brain")).toBe(true);
      expect(svg.querySelector("mask")).toBeNull();
      continue;
    }
    const paths = rootPaths(svg);
    expect(paths).toHaveLength(2);
    for (const [half, side] of (["left", "right"] as const).entries()) {
      const solid = variant === "filled" || variant === side;
      const path = paths[half]!;
      expect(path.getAttribute("fill")).toBe(solid ? "currentColor" : "none");
      expect(path.getAttribute("stroke")).toBe("currentColor");
      expect(path.getAttribute("stroke-width")).toBe("1.75");
      expect(path.getAttribute("d")?.startsWith(half === 0 ? "M10.2 3.6" : "M13.8 3.6")).toBe(true);
      expect(path.getAttribute("d")?.endsWith(half === 0 ? "10.2 20.4Z" : "13.8 20.4Z")).toBe(true);
      if (!solid) {
        expect(path.hasAttribute("mask")).toBe(false);
        continue;
      }
      const mask = Array.from(svg.querySelectorAll("mask")).find(
        (candidate) => path.getAttribute("mask") === `url(#${candidate.id})`,
      )!;
      expect(mask).toBeDefined();
      expect(mask.getAttribute("maskUnits")).toBe("userSpaceOnUse");
      const grooves = Array.from(mask.querySelectorAll("path"));
      expect(grooves.map((groove) => groove.getAttribute("d"))).toEqual(
        half === 0
          ? ["M10.2 12C8.4 12 7 10.9 6.6 9.3", "M3.5 14.8C5.1 14.6 6.5 15.4 7.1 16.9"]
          : ["M13.8 12C15.6 12 17 10.9 17.4 9.3", "M20.5 14.8C18.9 14.6 17.5 15.4 16.9 16.9"],
      );
      for (const groove of grooves) {
        expect(groove.getAttribute("fill")).toBe("none");
        expect(groove.getAttribute("stroke")).toBe("var(--color-black, #000)");
        expect(getComputedStyle(groove).stroke).toBe("rgb(0, 0, 0)");
        expect(groove.getAttribute("stroke-width")).toBe("1.7");
        expect(groove.getAttribute("stroke-linecap")).toBe("round");
      }
    }
  }
});

it("gives every house and brain its own mask references, including after a comparison changes", async () => {
  const ui = (value: SidebarAppearance) => (
    <SidebarAppearanceOverride value={value}>
      {symbol("Laptop")}
      {symbol("Laptop")}
      {symbol("VPS")}
      {symbol("VPS")}
    </SidebarAppearanceOverride>
  );
  await using screen = await render(ui({ ...DEFAULT_SIDEBAR_APPEARANCE, vpsSymbol: "filled" }), {
    router: false,
  });
  const assertReferences = () => {
    const masks = Array.from(screen.container.querySelectorAll("mask"));
    expect(masks).toHaveLength(6);
    expect(new Set(masks.map((mask) => mask.id)).size).toBe(6);
    for (const svg of screen.container.querySelectorAll("svg")) {
      for (const path of rootPaths(svg)) {
        const reference = path.getAttribute("mask");
        if (reference === null) continue;
        const matching = masks.filter((mask) => reference === `url(#${mask.id})`);
        expect(matching).toHaveLength(1);
        expect(matching[0]!.closest("svg")).toBe(svg);
      }
    }
  };
  assertReferences();
  await screen.rerender(ui({ ...DEFAULT_SIDEBAR_APPEARANCE, vpsSymbol: "right" }));
  assertReferences();
  const house = svgAt(screen.container, 0);
  expect(rootPaths(house)[0]!.getAttribute("stroke-width")).toBe("2");
  expect(house.querySelector("mask path")!.getAttribute("d")).toBe(
    "M10 23v-9.5a.5.5 0 0 1 .5-.5h3a.5.5 0 0 1 .5.5V23z",
  );
});

it.each(["rgb(24, 24, 27)", "rgb(244, 244, 245)"])(
  "paints the correct half at actual 12px with %s ink",
  async (color) => {
    await using screen = await render(
      <div style={{ color }}>
        {(["left", "right", "filled"] as const).map((vpsSymbol) => (
          <SidebarAppearanceOverride
            key={vpsSymbol}
            value={{ ...DEFAULT_SIDEBAR_APPEARANCE, vpsSymbol }}
          >
            {symbol("VPS")}
          </SidebarAppearanceOverride>
        ))}
        <SidebarAppearanceOverride value={DEFAULT_SIDEBAR_APPEARANCE}>
          {symbol("Laptop")}
        </SidebarAppearanceOverride>
      </div>,
      { router: false },
    );
    const [left, right, filled, house] = await Promise.all(
      [0, 1, 2, 3].map((index) => raster(svgAt(screen.container, index))),
    );
    expect(halfInk(left!, "left")).toBeGreaterThan(halfInk(right!, "left"));
    expect(halfInk(right!, "right")).toBeGreaterThan(halfInk(left!, "right"));
    expect(halfInk(filled!, "left")).toBe(halfInk(left!, "left"));
    expect(halfInk(filled!, "right")).toBe(halfInk(right!, "right"));
    const uncut = await raster(svgAt(screen.container, 2), true);
    expect(halfInk(filled!, "left")).toBeLessThan(halfInk(uncut, "left"));
    expect(halfInk(filled!, "right")).toBeLessThan(halfInk(uncut, "right"));
    for (const x of [5, 6]) {
      expect(filled!.data[(6 * 12 + x) * 4 + 3]).toBeLessThan(200);
    }
    expect(house!.data[(8 * 12 + 3) * 4 + 3]).toBe(255);
    expect(house!.data[(8 * 12 + 5) * 4 + 3]).toBe(0);
  },
);

it("preserves Mini, test, unknown, kind fallback and explicit icon overrides", async () => {
  const devices = () => (
    <>
      {symbol("Mini")}
      {symbol("Test Server")}
      {symbol("Unrecognized host")}
      <PersonalMachineSymbol kind="mac-mini" fallback={ServerIcon} width={12} height={12} />
      <PersonalMachineSymbol kind="laptop" fallback={ServerIcon} size={12} />
      <EnvironmentMachineIcon label="VPS" kind="cloud" override size={12} />
      <EnvironmentMachineIcon label="Laptop" kind="laptop" override size={12} />
    </>
  );
  await using screen = await render(
    <>
      {devices()}
      <SidebarAppearanceOverride value={DEFAULT_SIDEBAR_APPEARANCE}>
        {devices()}
      </SidebarAppearanceOverride>
    </>,
    { router: false },
  );
  for (let index = 0; index < 7; index++) {
    expect(svgAt(screen.container, index + 7).outerHTML).toBe(
      svgAt(screen.container, index).outerHTML,
    );
  }
  expect(svgAt(screen.container, 1).classList.contains("lucide-bot")).toBe(true);
  expect(svgAt(screen.container, 2).classList.contains("lucide-server")).toBe(true);
  expect(svgAt(screen.container, 5).classList.contains("lucide-cloud")).toBe(true);
});

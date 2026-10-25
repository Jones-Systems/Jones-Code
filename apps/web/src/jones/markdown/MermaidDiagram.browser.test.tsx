import { Suspense, use } from "react";
import { describe, expect, it, vi } from "vite-plus/test";

import { act, render } from "../../../test/render";
import { MarkdownMermaidCodeBlock } from "../../components/chat/MarkdownMermaidCodeBlock";
import { MermaidDiagram } from "../../components/chat/MermaidDiagram";

type MountOptions = {
  links: boolean;
  images: boolean;
  zoom: boolean;
  autoDirection: boolean;
  math?: unknown;
  sanitize: (svg: string) => Node;
  onRender: () => void;
  onError: (error: unknown) => void;
};
type Mount = (
  container: HTMLElement,
  source: string,
  options: MountOptions,
) => { destroy: () => void };

const engine = vi.hoisted(() => ({
  detectType: vi.fn<(source: string) => string>(),
  load: vi.fn<(type: string) => Promise<void>>(),
  mount: vi.fn<Mount>(),
}));

// Keep the component, React lifecycle and DOMPurify real; control only the engine boundary.
vi.mock("pele/lazy", () => engine);

const firstSource = "flowchart TD\n  Alpha --> Beta";
const secondSource = "flowchart TD\n  Gamma --> Delta";
const safeSvg = '<svg xmlns="http://www.w3.org/2000/svg"><text>Diagram label</text></svg>';
let nextFixture = 0;

function setup({ svg = safeSvg, invalidSource }: { svg?: string; invalidSource?: string } = {}) {
  // Distinct mock chunk keys isolate the component's loader cache without resetting React modules.
  // All sources within a case share one key, so source changes cannot hide behind a new load.
  const type = `acceptance-chunk-${++nextFixture}`;
  const events: string[] = [];
  const instances: {
    source: string;
    container: HTMLElement;
    options: MountOptions;
    destroy: ReturnType<typeof vi.fn>;
  }[] = [];

  engine.detectType.mockReset().mockImplementation((source) => {
    if (source === invalidSource) throw new Error("Unknown diagram type");
    return type;
  });
  engine.load.mockReset().mockResolvedValue(undefined);
  engine.mount.mockReset().mockImplementation((container, source, options) => {
    events.push(`mount:${source}`);
    const destroy = vi.fn(() => {
      events.push(`destroy:${source}`);
      container.replaceChildren();
    });
    instances.push({ source, container, options, destroy });
    if (source === invalidSource) {
      options.onError(new Error("Invalid diagram source"));
    } else {
      container.replaceChildren(options.sanitize(svg));
      options.onRender();
    }
    return { destroy };
  });

  return {
    events,
    instances,
    [Symbol.dispose]() {
      engine.detectType.mockReset();
      engine.load.mockReset();
      engine.mount.mockReset();
    },
  };
}

function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((onResolve, onReject) => {
    resolve = onResolve;
    reject = onReject;
  });
  return { promise, resolve, reject, [Symbol.dispose]: resolve };
}

function pendingDiagram(source: string, onExpand = vi.fn()) {
  return (
    <Suspense fallback={<p>Loading diagram</p>}>
      <MermaidDiagram source={source} onExpand={onExpand} />
    </Suspense>
  );
}

function expectNoActiveContent(svg: SVGSVGElement) {
  expect(svg.querySelector("script, style, a, image, img")).toBeNull();
  for (const element of [svg, ...svg.querySelectorAll("*")]) {
    expect(element.hasAttribute("href")).toBe(false);
    expect(element.hasAttribute("xlink:href")).toBe(false);
    expect(element.hasAttribute("src")).toBe(false);
    expect(element.hasAttribute("srcset")).toBe(false);
    expect(element.getAttributeNames().filter((name) => name.startsWith("on"))).toEqual([]);
  }
}

describe("real MermaidDiagram acceptance", () => {
  it("balances mounts across source changes and navigation without remounting unchanged source", async () => {
    using fixture = setup();
    await using screen = await render(pendingDiagram(`  ${firstSource}\n`), { router: false });
    await vi.waitFor(() => expect(fixture.instances).toHaveLength(1));
    const initial = fixture.instances[0]!;
    expect(initial.source).toBe(firstSource);
    expect(initial.container.isConnected).toBe(true);
    expect(initial.options).toMatchObject({
      links: false,
      images: false,
      zoom: false,
      autoDirection: false,
    });
    expect(screen.container.querySelector("svg text")?.textContent).toBe("Diagram label");

    await screen.rerender(pendingDiagram(firstSource, vi.fn()));
    expect(fixture.instances).toHaveLength(1);
    expect(initial.destroy).not.toHaveBeenCalled();

    await screen.rerender(pendingDiagram(secondSource));
    expect(fixture.events).toEqual([
      `mount:${firstSource}`,
      `destroy:${firstSource}`,
      `mount:${secondSource}`,
    ]);
    expect(engine.load).toHaveBeenCalledTimes(1);
    expect(fixture.instances[1]!.source).toBe(secondSource);

    await screen.rerender(<p>Another thread</p>);
    expect(screen.container.querySelector("svg")).toBeNull();
    await screen.rerender(pendingDiagram(firstSource));
    expect(fixture.instances).toHaveLength(3);
    await screen.unmount();
    for (const instance of fixture.instances) expect(instance.destroy).toHaveBeenCalledTimes(1);
    expect(fixture.events).toEqual([
      `mount:${firstSource}`,
      `destroy:${firstSource}`,
      `mount:${secondSource}`,
      `destroy:${secondSource}`,
      `mount:${firstSource}`,
      `destroy:${firstSource}`,
    ]);
  });

  it("never mounts an abandoned Suspense render even after its pending work resolves", async () => {
    using fixture = setup();
    using commit = deferred();
    // Resolve the real component's cached loader before testing suspension after its render.
    await using screen = await render(pendingDiagram(firstSource), { router: false });
    await vi.waitFor(() => expect(fixture.instances).toHaveLength(1));
    await screen.rerender(<p>Another thread</p>);
    expect(fixture.instances[0]!.destroy).toHaveBeenCalledTimes(1);
    const completedLifecycle = [...fixture.events];
    engine.detectType.mockClear();
    const reachedSibling = vi.fn();
    function HoldCommit() {
      reachedSibling();
      use(commit.promise);
      return null;
    }
    await screen.rerender(
      <Suspense fallback={<p>Loading diagram</p>}>
        <MermaidDiagram source={firstSource} onExpand={vi.fn()} />
        <HoldCommit />
      </Suspense>,
    );
    expect(engine.detectType).toHaveBeenCalledWith(firstSource);
    expect(reachedSibling).toHaveBeenCalled();
    expect(screen.container.textContent).toBe("Loading diagram");
    expect(fixture.events).toEqual(completedLifecycle);
    await screen.rerender(<p>Another thread</p>);
    await act(() => commit.resolve());
    expect(screen.container.textContent).toBe("Another thread");
    expect(fixture.events).toEqual(completedLifecycle);
  });

  it("exposes source and Retry after a chunk load failure, then mounts on explicit retry", async () => {
    using fixture = setup();
    using loader = deferred();
    engine.load.mockReturnValueOnce(loader.promise);
    await using screen = await render(pendingDiagram(firstSource), { router: false });
    await vi.waitFor(() => expect(engine.load).toHaveBeenCalledTimes(1));
    await act(() => loader.reject(new Error("Chunk unavailable")));
    await vi.waitFor(() =>
      expect(screen.container.textContent).toContain("The diagram renderer failed to load."),
    );
    expect(screen.container.querySelector("pre")?.textContent).toBe(firstSource);
    expect(fixture.instances).toHaveLength(0);
    const retry = screen.getByRole("button", { name: "Retry", exact: true });
    await act(() => retry.click());
    await vi.waitFor(() => expect(fixture.instances).toHaveLength(1));
    expect(engine.load).toHaveBeenCalledTimes(2);
    expect(screen.container.querySelector("pre")).toBeNull();
    expect(
      screen.container.querySelector<HTMLButtonElement>('button[aria-label="Expand diagram"]')
        ?.hidden,
    ).toBe(false);
    await screen.unmount();
    expect(fixture.instances[0]!.destroy).toHaveBeenCalledTimes(1);
  });

  it("shows invalid source without Retry and recovers when the source changes", async () => {
    const invalidSource = "not a diagram";
    using fixture = setup({ invalidSource });
    await using screen = await render(pendingDiagram(invalidSource), { router: false });
    await vi.waitFor(() =>
      expect(screen.container.textContent).toContain("Invalid diagram source"),
    );
    expect(screen.container.querySelector("pre")?.textContent).toBe(invalidSource);
    expect(
      [...screen.container.querySelectorAll("button")].some(
        (button) => button.textContent?.trim() === "Retry",
      ),
    ).toBe(false);
    expect(
      screen.container.querySelector<HTMLButtonElement>('button[aria-label="Expand diagram"]')
        ?.hidden,
    ).toBe(true);
    await screen.rerender(pendingDiagram(firstSource));
    await vi.waitFor(() => expect(fixture.instances).toHaveLength(2));
    expect(fixture.instances[0]!.destroy).toHaveBeenCalledTimes(1);
    expect(screen.container.querySelector("pre")).toBeNull();
    expect(
      screen.container.querySelector<HTMLButtonElement>('button[aria-label="Expand diagram"]')
        ?.hidden,
    ).toBe(false);
    expect(screen.container.querySelector("svg text")?.textContent).toBe("Diagram label");
    await screen.unmount();
    expect(fixture.instances[1]!.destroy).toHaveBeenCalledTimes(1);
  });

  it.each([
    { kind: "SVG", math: false },
    { kind: "MathML", math: true },
  ])("sanitizes actual $kind output while preserving labels", async ({ math }) => {
    const svg = `<svg xmlns="http://www.w3.org/2000/svg" onload="void 0">
      <text href="#unsafe" onclick="void 0">Safe SVG label</text>
      <use xmlns:xlink="http://www.w3.org/1999/xlink" xlink:href="#unsafe" />
      <script>void 0</script><style>.unused { color: red }</style>
      <a href="#unsafe"><text>Linked label</text></a>
      <image href="data:image/png,unsafe" />
      <foreignObject width="100" height="40"><div xmlns="http://www.w3.org/1999/xhtml">
        <img src="data:image/png,unsafe" srcset="data:image/png,unsafe" onerror="void 0" />
        <math xmlns="http://www.w3.org/1998/Math/MathML">
          <msup><mi mathvariant="bold" href="#unsafe">x</mi><mn>2</mn></msup>
          <mfrac linethickness="0"><mi>y</mi><mn>3</mn></mfrac>
          <mglyph src="data:image/png,unsafe" />
        </math>
      </div></foreignObject>
    </svg>`;
    using fixture = setup({ svg });
    const source = math ? 'flowchart TD\n  A["$$x^2$$"]' : firstSource;
    await using screen = await render(pendingDiagram(source), { router: false });
    await vi.waitFor(() => expect(fixture.instances).toHaveLength(1));
    const output = screen.container.querySelector("svg")!;
    expect(output).not.toBeNull();
    expect(output.querySelector("text")?.textContent).toBe("Safe SVG label");
    expectNoActiveContent(output);
    expect(fixture.instances[0]!.options.math !== undefined).toBe(math);
    if (math) {
      expect(output.querySelector("foreignObject")).not.toBeNull();
      expect(output.querySelector("math")?.namespaceURI).toBe("http://www.w3.org/1998/Math/MathML");
      expect(output.querySelector("msup > mi")?.textContent).toBe("x");
      expect(output.querySelector("msup > mi")?.getAttribute("mathvariant")).toBe("bold");
      expect(output.querySelector("mfrac")?.getAttribute("linethickness")).toBe("0");
      expect(output.querySelector("mglyph")).toBeNull();
    } else {
      expect(output.querySelector("foreignObject, math")).toBeNull();
    }
  });

  it("destroys the real diagram in source view and remounts it when toggled back", async () => {
    using fixture = setup();
    await using screen = await render(
      <MarkdownMermaidCodeBlock
        code={firstSource}
        fenceTitle={null}
        theme="light"
        isStreaming={false}
        onExpand={vi.fn()}
      >
        <pre>{firstSource}</pre>
      </MarkdownMermaidCodeBlock>,
      { router: false },
    );
    await vi.waitFor(() => expect(fixture.instances).toHaveLength(1));
    expect(
      screen.container.querySelector("[data-markdown-copy]")?.getAttribute("data-markdown-copy"),
    ).toContain(firstSource);
    await screen.getByRole("button", { name: "Show code", exact: true }).click();
    expect(fixture.instances[0]!.destroy).toHaveBeenCalledTimes(1);
    expect(screen.container.querySelector("[data-markdown-mermaid]")).toBeNull();
    expect(screen.container.querySelector("pre")?.textContent).toBe(firstSource);
    await screen.getByRole("button", { name: "Show diagram", exact: true }).click();
    await vi.waitFor(() => expect(fixture.instances).toHaveLength(2));
    expect(screen.container.querySelector("pre")).toBeNull();
    expect(screen.container.querySelector("[data-markdown-mermaid] svg")).not.toBeNull();
    expect(engine.load).toHaveBeenCalledTimes(1);
    await screen.unmount();
    expect(fixture.instances[1]!.destroy).toHaveBeenCalledTimes(1);
  });
});

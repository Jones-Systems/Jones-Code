import { expect, it, vi } from "vite-plus/test";
import { render } from "../../../test/render";
import { GhosttyTerminalSurface } from "../../terminal/ghostty/surface";
import { TerminalContentsFrame } from "./TerminalContentsFrame";
import "../../index.css";

it("copies current terminal contents on each click and shows the message-copy confirmation", async () => {
  const writeText = vi.fn(async (_text: string) => {});
  const clipboard = Object.getOwnPropertyDescriptor(navigator, "clipboard");
  Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });
  let terminal: GhosttyTerminalSurface | undefined;
  try {
    await using screen = await render(
      <div style={{ height: 256, width: 360 }}>
        <TerminalContentsFrame getText={() => terminal?.getText() ?? ""}>
          <div data-terminal-mount className="relative min-h-0 flex-1" />
        </TerminalContentsFrame>
      </div>,
      { router: false },
    );
    terminal = await GhosttyTerminalSurface.create(
      screen.container.querySelector<HTMLElement>("[data-terminal-mount]")!,
      {
        theme: {
          foreground: { r: 255, g: 255, b: 255 },
          background: { r: 0, g: 0, b: 0 },
          cursor: { r: 255, g: 255, b: 255 },
        },
        visible: true,
        onData: () => {},
        onResize: () => {},
        onSelectionChange: () => {},
        beforeKey: () => false,
        onLinkActivate: () => {},
      },
    );
    terminal.write("\x1b[32mInstall succeeded\x1b[0m\r\n");
    const copy = screen.getByRole("button", { name: "Copy terminal contents" });
    await copy.click();
    await expect.poll(() => writeText.mock.lastCall?.[0].trimEnd()).toBe("Install succeeded");
    await expect.element(copy).toBeDisabled();
    await expect.element(copy).toBeEnabled();
    terminal.write("latest result");
    await copy.click();
    await expect
      .poll(() => writeText.mock.lastCall?.[0].trimEnd())
      .toBe("Install succeeded\nlatest result");
  } finally {
    terminal?.dispose();
    if (clipboard) Object.defineProperty(navigator, "clipboard", clipboard);
    else Reflect.deleteProperty(navigator, "clipboard");
  }
});

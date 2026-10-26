import { expect, it, vi } from "vite-plus/test";
import { GhosttyTerminalCore } from "../../terminal/ghostty/core";

vi.mock("../../terminal/ghostty/vendor/ghostty-vt.wasm?url", async () => ({
  default: (await import("../../terminal/ghostty/vendor/ghostty-vt.wasm?inline")).default,
}));
vi.mock("../../terminal/ghostty/vendor/ghostty-write-pty.wasm?url&no-inline", async () => ({
  default: (await import("../../terminal/ghostty/vendor/ghostty-write-pty.wasm?inline")).default,
}));

it("copies scrollback and unwrapped plain text without changing the active selection", async () => {
  const core = await GhosttyTerminalCore.create(
    12,
    3,
    8,
    16,
    {
      foreground: { r: 255, g: 255, b: 255 },
      background: { r: 0, g: 0, b: 0 },
      cursor: { r: 255, g: 255, b: 255 },
    },
    () => {},
  );
  try {
    core.write("earlier\r\n\x1b[32mLong wrapped output 界🙂\x1b[0m\r\nlast\r\n");
    core.setSelection({ x: 0, y: 0 }, { x: 3, y: 0 });
    const selection = core.selectionText();
    expect(core.getText().trimEnd()).toBe("earlier\nLong wrapped output 界🙂\nlast");
    expect(core.selectionText()).toBe(selection);
    core.clearSelection();
    expect(core.getText().trimEnd()).toBe("earlier\nLong wrapped output 界🙂\nlast");
    expect(core.selectionText()).toBe("");
    core.write("fresh output");
    expect(core.getText()).toContain("fresh output");
  } finally {
    core.dispose();
  }
});

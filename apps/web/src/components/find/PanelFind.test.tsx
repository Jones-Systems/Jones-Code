// @vitest-environment jsdom
import { act, useEffect, useMemo } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vite-plus/test";
import { PanelFindProvider, PanelFindScope, usePanelFindSource } from "./PanelFind";

vi.mock("./panelFind", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./panelFind")>()),
  scrollFindRangeIntoView: vi.fn(),
}));
let root: Root;
let host: HTMLDivElement;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  vi.useFakeTimers();
  vi.spyOn(HTMLElement.prototype, "getClientRects").mockReturnValue([{}] as unknown as DOMRectList);
  host = document.createElement("div");
  document.body.append(host);
  root = createRoot(host);
});
afterEach(async () => {
  await act(async () => root.unmount());
  host.remove();
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});
async function frame() {
  await act(async () => {
    vi.advanceTimersByTime(32);
  });
}
async function key(target: EventTarget, key: string, extra: KeyboardEventInit = {}) {
  let handled = false;
  await act(async () => {
    const event = new KeyboardEvent("keydown", {
      key,
      ctrlKey: key === "f",
      bubbles: true,
      cancelable: true,
      ...extra,
    });
    target.dispatchEvent(event);
    handled = event.defaultPrevented;
  });
  await frame();
  return handled;
}
async function search(query: string) {
  const input = host.querySelector("input")!;
  await act(async () => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value")!.set!.call(input, query);
    input.dispatchEvent(new Event("input", { bubbles: true }));
  });
  await frame();
}

describe("focused panel find", () => {
  it("searches the clicked panel, wraps next/previous, closes, and restores focus", async () => {
    await act(async () =>
      root.render(
        <PanelFindProvider>
          <PanelFindScope label="main" data-find-default>
            <button>Main focus</button>
            <p>needle needle</p>
          </PanelFindScope>
          <PanelFindScope label="side">
            <button>Side focus</button>
            <p>needle</p>
          </PanelFindScope>
        </PanelFindProvider>,
      ),
    );
    const focus = host.querySelector("button")!;
    await act(async () => focus.focus());
    expect(await key(focus, "f")).toBe(true);
    await search("needle");
    expect(host.querySelector('[role="status"]')?.textContent).toBe("1 of 2");
    await key(document.activeElement!, "Enter");
    expect(host.querySelector('[role="status"]')?.textContent).toBe("2 of 2");
    await key(document.activeElement!, "Enter");
    expect(host.querySelector('[role="status"]')?.textContent).toBe("1 of 2");
    await key(document.activeElement!, "Enter", { shiftKey: true });
    expect(host.querySelector('[role="status"]')?.textContent).toBe("2 of 2");
    await key(document.activeElement!, "Escape");
    expect(host.querySelector('[role="search"]')).toBeNull();
    expect(document.activeElement).toBe(focus);
    const side = host.querySelector('[data-find-scope="side"] p')!;
    await act(async () => {
      side.dispatchEvent(new Event("pointerdown", { bubbles: true }));
    });
    await key(document.body, "f");
    expect(host.querySelector('[role="search"]')?.getAttribute("aria-label")).toBe("Find in side");
    await search("needle");
    expect(host.querySelector('[role="status"]')?.textContent).toBe("1 of 1");
  });

  it("uses Command+F on macOS and chooses the nested thread when nothing is focused", async () => {
    vi.spyOn(navigator, "platform", "get").mockReturnValue("MacIntel");
    await act(async () =>
      root.render(
        <PanelFindProvider>
          <PanelFindScope label="page" data-find-default>
            <PanelFindScope label="thread" data-find-default>
              <p>Message</p>
            </PanelFindScope>
          </PanelFindScope>
        </PanelFindProvider>,
      ),
    );
    expect(await key(document.body, "f", { ctrlKey: true })).toBe(false);
    expect(await key(document.body, "f", { ctrlKey: false, metaKey: true })).toBe(true);
    expect(host.querySelector('[role="search"]')?.getAttribute("aria-label")).toBe(
      "Find in thread",
    );
    await key(document.activeElement!, "Escape");
    const page = host.querySelector('[data-find-scope="page"]')!;
    await act(async () => {
      page.dispatchEvent(new Event("pointerdown", { bubbles: true }));
    });
    await key(document.body, "f", { ctrlKey: false, metaKey: true });
    expect(host.querySelector('[role="search"]')?.getAttribute("aria-label")).toBe(
      "Find in thread",
    );
  });

  it("finds unmounted messages through a source and requests each older page once", async () => {
    const reveal = vi.fn();
    const load = vi.fn();
    function Timeline() {
      const source = useMemo(
        () => ({
          getItems: () => [{ id: "older", text: "old needle" }],
          reveal,
          loading: false,
          history: { cursor: "older-page", load },
        }),
        [],
      );
      const state = usePanelFindSource(source);
      useEffect(() => {
        if (state.selectedId) reveal(`mounted:${state.selectedId}`);
      }, [state.selectedId]);
      return <p>Current message without the search term</p>;
    }
    await act(async () =>
      root.render(
        <PanelFindProvider>
          <PanelFindScope label="thread" data-find-default>
            <Timeline />
          </PanelFindScope>
        </PanelFindProvider>,
      ),
    );
    await key(document.body, "f");
    await search("needle");
    expect(host.querySelector('[role="status"]')?.textContent).toBe("1 of 1");
    expect(reveal).toHaveBeenCalledWith("older");
    expect(reveal).toHaveBeenCalledWith("mounted:older");
    expect(load).toHaveBeenCalledTimes(1);
    await key(document.activeElement!, "Enter");
    expect(load).toHaveBeenCalledTimes(1);
  });
});

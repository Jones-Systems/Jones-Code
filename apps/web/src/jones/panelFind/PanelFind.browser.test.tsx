import { useState } from "react";
import { expect, it } from "vite-plus/test";
import { page, userEvent } from "vite-plus/test/browser";
import { act, render } from "../../../test/render";
import { PanelFindProvider, PanelFindScope } from "./PanelFind";
import { collectFindRanges, findTextMatches } from "./findRanges";

async function find() {
  await act(() =>
    window.dispatchEvent(
      new KeyboardEvent("keydown", {
        key: "f",
        ctrlKey: !navigator.platform.includes("Mac"),
        metaKey: navigator.platform.includes("Mac"),
        bubbles: true,
        cancelable: true,
      }),
    ),
  );
}

it("searches the active panel, navigates matches, restores focus, and keeps button keyboard semantics", async () => {
  await using screen = await render(
    <PanelFindProvider>
      <PanelFindScope label="page" data-find-default>
        <button>Main</button>
        <p>needle needle</p>
      </PanelFindScope>
      <PanelFindScope label="side">
        <button>Side</button>
        <p>needle</p>
      </PanelFindScope>
    </PanelFindProvider>,
  );
  await userEvent.click(screen.getByRole("button", { name: "Main", exact: true }));
  await find();
  await userEvent.fill(page.getByRole("textbox", { name: "Find in page" }), "needle");
  await expect.element(page.getByRole("status")).toHaveTextContent("1 of 2");
  await userEvent.keyboard("{Enter}");
  await expect.element(page.getByRole("status")).toHaveTextContent("2 of 2");
  await userEvent.keyboard("{Enter}");
  await expect.element(page.getByRole("status")).toHaveTextContent("1 of 2");
  await userEvent.keyboard("{Shift>}{Enter}{/Shift}");
  await expect.element(page.getByRole("status")).toHaveTextContent("2 of 2");
  await userEvent.keyboard("{Escape}");
  await expect.element(screen.getByRole("button", { name: "Main", exact: true })).toHaveFocus();
  await userEvent.click(screen.getByRole("button", { name: "Side", exact: true }));
  await find();
  await userEvent.fill(page.getByRole("textbox", { name: "Find in side" }), "needle");
  await expect.element(page.getByRole("status")).toHaveTextContent("1 of 1");
  await userEvent.fill(page.getByRole("textbox", { name: "Find in side" }), "missing");
  await expect.element(page.getByRole("status")).toHaveTextContent("0 of 0");
  await expect.element(page.getByRole("button", { name: "Next match" })).toBeDisabled();
  await act(() => document.querySelector<HTMLButtonElement>('[aria-label="Close find"]')!.focus());
  await userEvent.keyboard("{Enter}");
  await expect.element(page.getByRole("search")).not.toBeInTheDocument();
});

it("keeps the focused panel searchable while a nonmodal notification is visible", async () => {
  await using screen = await render(
    <PanelFindProvider>
      <PanelFindScope label="page" data-find-default>
        <button>Page content</button>
        <p>needle</p>
      </PanelFindScope>
      <div role="dialog" aria-modal="false" aria-label="Provider notification">
        Provider updates available
      </div>
    </PanelFindProvider>,
  );
  await userEvent.click(screen.getByRole("button", { name: "Page content" }));
  await find();
  await expect.element(page.getByRole("textbox", { name: "Find in page" })).toBeVisible();
  await userEvent.fill(page.getByRole("textbox", { name: "Find in page" }), "needle");
  await expect.element(page.getByRole("status")).toHaveTextContent("1 of 1");
  await expect.element(screen.getByRole("dialog", { name: "Provider notification" })).toBeVisible();
});

it("routes a focused side thread into full-history search and returns to the page when removed", async () => {
  let current: string | null = null;
  function ui(side = true) {
    return (
      <PanelFindProvider>
        <PanelFindScope label="page" data-find-default>
          <PanelFindScope
            label="thread"
            data-find-default
            nativeFind={{
              open: () => {
                current = "main";
              },
              close: () => {
                if (current === "main") current = null;
              },
            }}
          >
            <button>Main thread</button>
          </PanelFindScope>
          {side && (
            <PanelFindScope label="side">
              <PanelFindScope
                label="side thread"
                nativeFind={{
                  open: () => {
                    current = "side";
                  },
                  close: () => {
                    if (current === "side") current = null;
                  },
                }}
              >
                <button>Side thread</button>
              </PanelFindScope>
            </PanelFindScope>
          )}
        </PanelFindScope>
      </PanelFindProvider>
    );
  }
  await using screen = await render(ui());
  await find();
  expect(current).toBe("main");
  await userEvent.click(screen.getByRole("button", { name: "Side thread" }));
  await find();
  expect(current).toBe("side");
  await expect.element(page.getByRole("search")).not.toBeInTheDocument();
  await screen.rerender(ui(false));
  await find();
  expect(current).toBe("main");
});

it("matches formatted text without searching hidden content, inputs, or adjacent panels", async () => {
  await using screen = await render(
    <PanelFindScope label="page">
      <p>
        A <strong>work</strong>tree setting
      </p>
      <p>work</p>
      <p>tree</p>
      <div hidden>worktree</div>
      <input value="worktree" readOnly />
      <PanelFindScope label="side">
        <p>worktree</p>
      </PanelFindScope>
    </PanelFindScope>,
  );
  const ranges = collectFindRanges(screen.container.firstElementChild as HTMLElement, "worktree");
  expect(ranges.map((range) => range.toString())).toEqual(["worktree"]);
  expect(findTextMatches("A [x] and [X]", "[x]")).toEqual([
    { start: 2, end: 5 },
    { start: 10, end: 13 },
  ]);
  expect(findTextMatches("İ Next next", "next")).toEqual([
    { start: 2, end: 6 },
    { start: 7, end: 11 },
  ]);
});

it("searches portaled Settings, then Escape closes find before the Settings dialog", async () => {
  const { AppRouteModal } = await import("../../components/AppRouteModal");
  let backgroundSearch = false;
  await using _screen = await render(
    <PanelFindProvider>
      <PanelFindScope
        label="thread"
        data-find-default
        nativeFind={{
          open: () => {
            backgroundSearch = true;
          },
          close: () => {
            backgroundSearch = false;
          },
        }}
      >
        <p>Background thread</p>
      </PanelFindScope>
      <AppRouteModal label="Settings">
        <p>Worktree needle</p>
      </AppRouteModal>
    </PanelFindProvider>,
  );
  await expect.element(page.getByRole("dialog", { name: "Settings" })).toBeVisible();
  await find();
  await userEvent.fill(page.getByRole("textbox", { name: "Find in settings" }), "needle");
  await expect.element(page.getByRole("search").getByRole("status")).toHaveTextContent("1 of 1");
  expect(backgroundSearch).toBe(false);
  await userEvent.keyboard("{Escape}");
  await expect.element(page.getByRole("search")).not.toBeInTheDocument();
  await expect.element(page.getByRole("dialog", { name: "Settings" })).toBeVisible();
});

it("dismisses embedded full-history find with Escape and restores its composer", async () => {
  function SideThread() {
    const [active, setActive] = useState(false);
    return (
      <PanelFindScope
        label="side thread"
        data-find-default
        nativeFind={{
          active,
          open: () => setActive(true),
          close: () => setActive(false),
          dismiss: () => {
            setActive(false);
            document.querySelector<HTMLTextAreaElement>("textarea")?.focus();
          },
        }}
      >
        <textarea aria-label="Side composer" />
        {active && <input aria-label="Native thread query" />}
      </PanelFindScope>
    );
  }
  await using screen = await render(
    <PanelFindProvider>
      <SideThread />
    </PanelFindProvider>,
  );
  await userEvent.click(screen.getByRole("textbox", { name: "Side composer" }));
  await find();
  await expect.element(screen.getByRole("textbox", { name: "Native thread query" })).toBeVisible();
  await userEvent.click(screen.getByRole("textbox", { name: "Native thread query" }));
  await userEvent.keyboard("{Escape}");
  await expect
    .element(screen.getByRole("textbox", { name: "Native thread query" }))
    .not.toBeInTheDocument();
  await expect.element(screen.getByRole("textbox", { name: "Side composer" })).toHaveFocus();
});

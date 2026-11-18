import "~/index.css";
import { useLayoutEffect, useRef } from "react";
import { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { scopeThreadRef, scopedThreadKey } from "@t3tools/client-runtime/environment";
import { expect, it, vi } from "vite-plus/test";

import { act, render } from "../../../test/render";
import { ThreadContextDragGhost } from "../../components/chat/ThreadContextDragGhost";
import {
  threadContextDropTargetProps,
  useThreadContextDrop,
} from "../../components/chat/threadContextDrag";
import type { SidebarListItem } from "../../components/Sidebar.logic";
import { locateSidebarTitlePlacement, showSidebarTitlePlacement } from "./sidebarTitlePlacement";
import { registerThreadTitleSidebar, useThreadTitleDrag } from "./threadTitleDrag";

const environment = EnvironmentId.make("title-drag");
const thread = scopeThreadRef(environment, ThreadId.make("source"));
const key = scopedThreadKey(thread);
const first = scopedThreadKey(scopeThreadRef(environment, ThreadId.make("first")));
const last = scopedThreadKey(scopeThreadRef(environment, ThreadId.make("last")));
const items: readonly SidebarListItem[] = [
  { kind: "marker", marker: "pinned-divider" },
  { kind: "thread", key: first, section: "active" },
  { kind: "thread", key, section: "active" },
  { kind: "thread", key: last, section: "active" },
  { kind: "marker", marker: "settled-header" },
];

function Fixture({
  insertMention,
  place,
}: {
  insertMention: (refs: readonly (typeof thread)[]) => void;
  place: (order: readonly string[]) => void;
}) {
  const sidebar = useRef<HTMLUListElement>(null);
  const composer = useRef<HTMLDivElement>(null);
  const pointerDown = useThreadTitleDrag(thread, "Source thread");
  useThreadContextDrop(composer, environment, "fixture", (refs, isActive) => {
    if (isActive()) insertMention(refs);
  });
  useLayoutEffect(() => {
    const list = sidebar.current!;
    let indicator: HTMLElement | null = null;
    return registerThreadTitleSidebar(list, {
      move: (point) => {
        indicator = showSidebarTitlePlacement(
          indicator,
          locateSidebarTitlePlacement(list, point, items, key, "active"),
        );
      },
      drop: (point) => {
        const position = locateSidebarTitlePlacement(list, point, items, key, "active");
        if (position) place(position.target.activeOrder);
      },
      clear: () => {
        indicator = showSidebarTitlePlacement(indicator, null);
      },
    });
  }, [place]);
  return (
    <>
      <ul
        ref={sidebar}
        data-thread-title-sidebar=""
        style={{ position: "fixed", top: 12, left: 12, width: 220 }}
      >
        {items.flatMap((item) =>
          item.kind === "thread" ? (
            <li
              key={item.key}
              data-thread-item={item.key}
              data-jones-sidebar-card=""
              style={{ height: 40 }}
            >
              Card {item.key}
            </li>
          ) : (
            []
          ),
        )}
      </ul>
      <button
        type="button"
        onPointerDown={pointerDown}
        style={{ position: "fixed", top: 12, left: 300 }}
      >
        Source thread
      </button>
      <div
        {...threadContextDropTargetProps()}
        ref={composer}
        style={{ position: "fixed", left: 300, top: 120, width: 240, height: 40 }}
      >
        Composer
      </div>
      <ThreadContextDragGhost />
    </>
  );
}

function pointer(
  target: EventTarget,
  type: "pointerdown" | "pointermove" | "pointerup",
  x: number,
  y: number,
) {
  return act(() => {
    target.dispatchEvent(
      new PointerEvent(type, {
        bubbles: true,
        cancelable: true,
        pointerId: 1,
        isPrimary: true,
        buttons: type === "pointerup" ? 0 : 1,
        clientX: x,
        clientY: y,
      }),
    );
  });
}

it("switches between the mention and the real sidebar card, then places the thread after a card", async () => {
  const insertMention = vi.fn();
  const place = vi.fn();
  await using screen = await render(<Fixture insertMention={insertMention} place={place} />);
  const source = screen.container.querySelector("button")!;
  await pointer(source, "pointerdown", 312, 18);
  await pointer(document, "pointermove", 312, 130);
  expect(document.querySelector('[data-thread-context-drag-preview="mention"]')?.textContent).toBe(
    "Source thread",
  );
  expect(document.querySelector("[data-thread-context-over]")?.textContent).toBe("Composer");

  await pointer(document, "pointermove", 120, 125);
  const card = document.querySelector("[data-personal-thread-drag]");
  expect(card?.textContent).toContain(key);
  expect(document.querySelector("[data-thread-context-drag-preview]")).toBeNull();
  expect(document.querySelector('[data-thread-title-insertion="after"]')).toHaveAttribute(
    "data-thread-item",
    last,
  );

  await pointer(document, "pointerup", 120, 125);
  expect(place).toHaveBeenCalledExactlyOnceWith([first, last, key]);
  expect(insertMention).not.toHaveBeenCalled();
  expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
  expect(document.querySelector("[data-thread-title-insertion]")).toBeNull();
});

it("inserts only the header thread after leaving the sidebar and supports Escape cancellation", async () => {
  const insertMention = vi.fn();
  const place = vi.fn();
  await using screen = await render(<Fixture insertMention={insertMention} place={place} />);
  const source = screen.container.querySelector("button")!;
  await pointer(source, "pointerdown", 312, 18);
  await pointer(document, "pointermove", 120, 18);
  expect(document.querySelector('[data-thread-title-insertion="before"]')).toHaveAttribute(
    "data-thread-item",
    first,
  );
  await pointer(document, "pointermove", 312, 130);
  expect(document.querySelector("[data-thread-title-insertion]")).toBeNull();
  await pointer(document, "pointerup", 312, 130);
  expect(insertMention).toHaveBeenCalledExactlyOnceWith([thread]);
  expect(place).not.toHaveBeenCalled();

  await pointer(source, "pointerdown", 312, 18);
  await pointer(document, "pointermove", 120, 18);
  await act(() => document.dispatchEvent(new KeyboardEvent("keydown", { code: "Escape" })));
  await pointer(document, "pointerup", 120, 18);
  expect(place).not.toHaveBeenCalled();
  expect(document.querySelector("[data-thread-title-insertion]")).toBeNull();
  expect(document.querySelector("[data-personal-thread-drag]")).toBeNull();
});

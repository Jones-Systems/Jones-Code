import { useLayoutEffect, useRef, type PointerEvent as ReactPointerEvent } from "react";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { scopedThreadKey } from "@t3tools/client-runtime/environment";

import { SIDEBAR_DRAG_DISTANCE, SidebarPointerSensor } from "../../components/Sidebar.pointer";
import {
  dropThreadContext,
  endThreadContextDrag,
  moveThreadContextDrag,
} from "../../components/chat/threadContextDrag";
import { liftPersonalThreadCard } from "../workstreams/personalThreadDrag";
import { createSidebarDragAutoScroll } from "../workstreams/sidebarDragAutoScroll";

type Point = { x: number; y: number };
type SidebarDrop = {
  move: (point: Point, thread: ScopedThreadRef) => void;
  drop: (point: Point, thread: ScopedThreadRef) => void;
  clear: () => void;
};

const targets = new WeakMap<HTMLElement, SidebarDrop>();

export function registerThreadTitleSidebar(element: HTMLElement, target: SidebarDrop) {
  targets.set(element, target);
  return () => {
    targets.delete(element);
    target.clear();
  };
}

export function useThreadTitleDrag(thread: ScopedThreadRef, title: string) {
  const sensor = useRef<SidebarPointerSensor | null>(null);
  const key = scopedThreadKey(thread);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Releases the document pointer listeners when the header unmounts or changes thread.
  useLayoutEffect(
    () => () => sensor.current?.cancel(),
    // oxlint-disable-next-line react/exhaustive-effect-dependencies -- A new scoped thread ends the previous title gesture.
    [key],
  );

  return (event: ReactPointerEvent<HTMLElement>) => {
    if (
      !event.isPrimary ||
      event.button !== 0 ||
      event.altKey ||
      event.ctrlKey ||
      event.metaKey ||
      event.shiftKey ||
      (event.target as Element).closest("[data-thread-title-chevron]")
    )
      return;

    const document = event.currentTarget.ownerDocument;
    const findSidebar = (point: Point) =>
      document
        .elementFromPoint(point.x, point.y)
        ?.closest<HTMLElement>("[data-thread-title-sidebar]") ?? null;
    let sidebar: HTMLElement | null = null;
    let target: SidebarDrop | undefined;
    let card: ReturnType<typeof liftPersonalThreadCard> | null = null;
    let scroll: ReturnType<typeof createSidebarDragAutoScroll> | null = null;

    const leaveSidebar = () => {
      target?.clear();
      scroll?.dispose();
      card?.dispose();
      sidebar = null;
      target = undefined;
      card = null;
      scroll = null;
    };
    const move = (point: Point) => {
      const nextSidebar = findSidebar(point);
      const nextTarget = nextSidebar ? targets.get(nextSidebar) : undefined;
      if (sidebar !== nextSidebar || target !== nextTarget) {
        leaveSidebar();
        if (nextSidebar && nextTarget) {
          sidebar = nextSidebar;
          target = nextTarget;
          const source = Array.from(
            sidebar.querySelectorAll<HTMLElement>("[data-jones-sidebar-card][data-thread-item]"),
          ).find((row) => row.dataset.threadItem === key);
          if (source) card = liftPersonalThreadCard(source, point, { followPointer: true });
          scroll = createSidebarDragAutoScroll(sidebar, (next) => target?.move(next, thread));
        }
      }
      if (target) {
        card?.move(point);
        scroll?.move(point);
        target.move(point, thread);
      }
      if (card) endThreadContextDrag();
      else
        moveThreadContextDrag(point, {
          title: title.trim() || "Thread",
          count: 1,
          sidebarCard: target !== undefined,
        });
    };

    sensor.current?.cancel();
    sensor.current = new SidebarPointerSensor({
      active: key,
      event: event.nativeEvent,
      options: {
        distance: SIDEBAR_DRAG_DISTANCE,
        onAttach: () => {},
        onDrop: (point) => {
          const destination = findSidebar(point);
          const sidebarDrop = destination ? targets.get(destination) : undefined;
          if (sidebarDrop) sidebarDrop.drop(point, thread);
          else dropThreadContext(point, [thread]);
          return true;
        },
        onFinish: () => {
          sensor.current = null;
          leaveSidebar();
          endThreadContextDrag();
        },
      },
      onMove: move,
      onPending: () => {},
      onStart: () => {},
      onEnd: () => {},
      onCancel: () => {},
      onAbort: () => {},
    });
  };
}

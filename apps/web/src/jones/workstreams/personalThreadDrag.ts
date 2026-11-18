import { copySidebarAppearanceScope } from "../sidebarAppearance/useSidebarAppearanceHost";
import {
  parseScopedThreadKey,
  scopeThreadRef,
  scopedThreadKey,
} from "@t3tools/client-runtime/environment";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import { useThreadSelectionStore } from "../../threadSelectionStore";

export function personalThreadContextRefs(thread: EnvironmentThreadShell) {
  const ref = scopeThreadRef(thread.environmentId, thread.id);
  const key = scopedThreadKey(ref);
  const selected = useThreadSelectionStore.getState().selectedThreadKeys;
  const keys = selected.has(key) ? [...selected] : [key];
  return keys.flatMap((candidate) => {
    const selectedRef = parseScopedThreadKey(candidate);
    return selectedRef ? [selectedRef] : [];
  });
}

export function liftPersonalThreadCard(
  source: HTMLElement,
  origin: { x: number; y: number },
  { followPointer = false } = {},
) {
  const rect = source.getBoundingClientRect();
  const view = document.createElement("div");
  view.dataset.personalThreadDrag = "";
  copySidebarAppearanceScope(source, view);
  view.setAttribute("aria-hidden", "true");
  view.inert = true;
  // Match the pinned row's lifted surface while retaining the rendered title card.
  view.className =
    "pointer-events-none fixed left-0 top-0 z-[100] rounded-md bg-sidebar bg-linear-to-b from-sidebar-row-active to-sidebar-row-active text-sidebar-foreground shadow-lg";
  view.style.width = `${rect.width}px`;
  const card = source.cloneNode(true) as HTMLElement;
  for (const node of [card, ...card.querySelectorAll<HTMLElement>("*")]) {
    node.removeAttribute("id");
    node.removeAttribute("data-personal-thread");
    node.removeAttribute("data-thread-item");
    node.removeAttribute("data-sidebar-row");
    node.style.setProperty("pointer-events", "none", "important");
  }
  view.append(card);
  document.body.append(view);
  const opacity = source.style.opacity;
  if (!followPointer) {
    source.style.opacity = "0.3";
    source.dataset.personalThreadDragSource = "";
  }
  const move = (point: { x: number; y: number }) => {
    const x = followPointer ? point.x + 12 : rect.left;
    const y = followPointer ? point.y + 12 : rect.top + point.y - origin.y;
    view.style.transform = `translate3d(${x}px, ${y}px, 0)`;
  };
  move(origin);
  return {
    move,
    setVisible: (visible: boolean) => {
      view.hidden = !visible;
    },
    dispose: () => {
      view.remove();
      if (!followPointer) {
        source.style.opacity = opacity;
        delete source.dataset.personalThreadDragSource;
      }
    },
  };
}

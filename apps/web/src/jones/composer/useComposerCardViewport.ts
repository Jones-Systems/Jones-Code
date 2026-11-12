import { useLayoutEffect, useRef, useState } from "react";
import { useStableCallback } from "../../hooks/useStableCallback";
import { observeResize } from "../../lib/observeResize";

function composerSiblingElements(column: HTMLElement, overlay: HTMLElement, workspace: Element) {
  const observed: Element[] = [workspace];
  for (
    let node: HTMLElement = column;
    node !== overlay && node.parentElement;
    node = node.parentElement
  ) {
    observed.push(...Array.from(node.parentElement.children).filter((sibling) => sibling !== node));
  }
  return observed;
}

/** Keep attached cards above the editor inside its actual conversation area.
 * The editor and external rails keep their geometry; overflowing cards scroll
 * in their existing order rather than painting over the conversation header. */
export function useComposerCardViewport(workspace: HTMLDivElement | null) {
  const ref = useRef<HTMLDivElement | null>(null);
  const [maxHeight, setMaxHeight] = useState<number>();
  const measure = useStableCallback(() => {
    const column = ref.current;
    if (!column || !workspace) return;
    const overlay = column.closest<HTMLElement>("[data-chat-composer-overlay]");
    if (!overlay) return;
    const padding = Number.parseFloat(getComputedStyle(overlay).paddingTop) || 0;
    const top = Math.max(
      0,
      workspace.getBoundingClientRect().top,
      window.visualViewport?.offsetTop ?? 0,
    );
    const available = Math.max(0, column.getBoundingClientRect().bottom - top - padding);
    const next = column.scrollHeight > available ? available : undefined;
    setMaxHeight((previous) => (previous === next ? previous : next));
  });

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Measure committed card bounds and own the resize/content subscriptions that keep the editor's attached stack within the conversation.
  useLayoutEffect(() => {
    const column = ref.current;
    if (!column || !workspace) return;
    const overlay = column.closest<HTMLElement>("[data-chat-composer-overlay]");
    if (!overlay) return;
    measure();
    // Natural sibling branches include the editor and external rails. Their
    // size changes move the dock; observing our constrained column/overlay
    // instead would feed our own height writes back into ResizeObserver.
    const observed = composerSiblingElements(column, overlay, workspace);
    const stop = observeResize(observed, measure);
    let stopChildren = observeResize(Array.from(column.children), measure);
    const mutations = new MutationObserver(() => {
      stopChildren();
      stopChildren = observeResize(Array.from(column.children), measure);
      measure();
    });
    mutations.observe(column, { childList: true });
    window.addEventListener("resize", measure);
    window.visualViewport?.addEventListener("resize", measure);
    window.visualViewport?.addEventListener("scroll", measure);
    document.fonts?.addEventListener("loadingdone", measure);
    return () => {
      stop();
      stopChildren();
      mutations.disconnect();
      window.removeEventListener("resize", measure);
      window.visualViewport?.removeEventListener("resize", measure);
      window.visualViewport?.removeEventListener("scroll", measure);
      document.fonts?.removeEventListener("loadingdone", measure);
    };
  }, [workspace, measure]);

  return {
    ref,
    tabIndex: maxHeight === undefined ? undefined : 0,
    style:
      maxHeight === undefined
        ? undefined
        : {
            maxHeight,
            overflowY: "auto" as const,
            overscrollBehavior: "contain" as const,
          },
  };
}

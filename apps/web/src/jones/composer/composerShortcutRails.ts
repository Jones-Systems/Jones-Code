import { useLayoutEffect, useRef, useState, type RefObject } from "react";
import { observeResize } from "~/lib/observeResize";

const COMPOSER_SHORTCUT_MAX_HEIGHT_PX = 144;

export function resolveComposerShortcutRailsVisibility(input: {
  eligible: boolean;
  formWidth: number;
  hostWidth: number;
  conversationHeight: number;
  groupWidths: readonly number[];
  groupHeights: readonly number[];
}): boolean {
  const widths = input.groupWidths.filter((width) => width > 0);
  const bandHeight = Math.max(0, ...input.groupHeights);
  if (
    !input.eligible ||
    input.formWidth <= 0 ||
    input.hostWidth <= 0 ||
    input.conversationHeight <= 0 ||
    widths.length === 0 ||
    bandHeight <= 0
  ) {
    return false;
  }
  // The external controls remain available in compact and short layouts. The
  // band wraps whole groups and scrolls overflowing content instead of hiding it.
  return true;
}

export function useComposerShortcutRails(input: {
  host: HTMLDivElement | null;
  workspace: HTMLDivElement | null;
  formRef: RefObject<HTMLFormElement | null>;
  bandRef: RefObject<HTMLDivElement | null>;
  accountGroupRef: RefObject<HTMLDivElement | null>;
  effortGroupRef: RefObject<HTMLDivElement | null>;
  eligible: boolean;
  contentKey: unknown;
}) {
  const {
    host,
    workspace,
    formRef,
    bandRef,
    accountGroupRef,
    effortGroupRef,
    eligible,
    contentKey,
  } = input;
  const [layout, setLayout] = useState({ visible: false, height: 0 });
  const measure = () => {
    const form = formRef.current;
    const band = bandRef.current;
    if (!host || !workspace || !form || !band) return;
    const groups = [accountGroupRef.current, effortGroupRef.current];
    const groupSizes = groups.map((group) => group?.getBoundingClientRect());
    const groupWidths = groupSizes.map((size) => size?.width ?? 0);
    const groupHeights = groupSizes.map((size) => size?.height ?? 0);
    const overlay = host.closest<HTMLElement>("[data-chat-composer-overlay]");
    if (!overlay) return;
    const hostStyle = getComputedStyle(host);
    const hostWidth =
      host.clientWidth -
      (Number.parseFloat(hostStyle.paddingInlineStart) || 0) -
      (Number.parseFloat(hostStyle.paddingInlineEnd) || 0);
    const visible = resolveComposerShortcutRailsVisibility({
      eligible,
      formWidth: form.clientWidth,
      hostWidth,
      conversationHeight: workspace.clientHeight,
      groupWidths,
      groupHeights,
    });
    const content = band.querySelector<HTMLElement>("[data-chat-composer-shortcut-content]");
    const height = visible
      ? Math.min(
          content?.getBoundingClientRect().height ?? Math.max(0, ...groupHeights),
          COMPOSER_SHORTCUT_MAX_HEIGHT_PX,
          Math.max(48, Math.floor(workspace.clientHeight / 3)),
        )
      : 0;
    setLayout((previous) =>
      previous.visible === visible && previous.height === height ? previous : { visible, height },
    );
  };

  const measuredContent = useRef(contentKey);
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Remeasure composer rails after committed content changes alter their geometry.
  useLayoutEffect(() => {
    if (measuredContent.current !== contentKey) {
      measuredContent.current = contentKey;
      measure();
    }
  }, [contentKey, measure]);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Own resize and font-loading subscriptions for composer rail geometry and release them on unmount.
  useLayoutEffect(() => {
    measure();
    if (typeof ResizeObserver === "undefined") return;
    const elements = [
      host,
      workspace,
      formRef.current,
      bandRef.current,
      accountGroupRef.current,
      effortGroupRef.current,
      host?.closest<HTMLElement>("[data-chat-composer-overlay]"),
    ].filter((element): element is HTMLElement => element != null);
    const stopResize = observeResize(elements, measure);
    document.fonts?.addEventListener("loadingdone", measure);
    return () => {
      stopResize();
      document.fonts?.removeEventListener("loadingdone", measure);
    };
  }, [host, workspace, formRef, bandRef, accountGroupRef, effortGroupRef, measure]);

  return { visible: eligible && layout.visible, height: eligible ? layout.height : 0 };
}

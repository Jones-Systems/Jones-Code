import { useCallback, useLayoutEffect, useRef, useState, type RefObject } from "react";
import { shouldUseCompactComposerFooter } from "../composerFooterLayout";

const COMPOSER_SHORTCUT_GROUP_GAP_PX = 16;
const COMPOSER_SHORTCUT_CONVERSATION_MIN_PX = 160;

export function resolveComposerShortcutRailsVisibility(input: {
  eligible: boolean;
  formWidth: number;
  hostWidth: number;
  conversationHeight: number;
  composerHeight: number;
  currentBandHeight: number;
  groupWidths: readonly number[];
  groupHeights: readonly number[];
  hasWideActions: boolean;
  previousVisible: boolean;
}): boolean {
  const widths = input.groupWidths.filter((width) => width > 0);
  const bandHeight = Math.max(0, ...input.groupHeights);
  if (
    !input.eligible ||
    input.formWidth <= 0 ||
    input.hostWidth <= 0 ||
    input.conversationHeight <= 0 ||
    widths.length === 0 ||
    bandHeight <= 0 ||
    shouldUseCompactComposerFooter(input.formWidth, { hasWideActions: input.hasWideActions })
  ) {
    return false;
  }
  const slack = input.previousVisible ? 0 : 1;
  const requiredWidth =
    widths.reduce((total, width) => total + width, 0) +
    Math.max(0, widths.length - 1) * COMPOSER_SHORTCUT_GROUP_GAP_PX;
  const baseComposerHeight = Math.max(0, input.composerHeight - input.currentBandHeight);
  return (
    input.hostWidth >= requiredWidth + slack &&
    input.conversationHeight >=
      baseComposerHeight + bandHeight + COMPOSER_SHORTCUT_CONVERSATION_MIN_PX + slack
  );
}

export function useComposerShortcutRails(input: {
  host: HTMLDivElement | null;
  workspace: HTMLDivElement | null;
  formRef: RefObject<HTMLFormElement | null>;
  bandRef: RefObject<HTMLDivElement | null>;
  accountGroupRef: RefObject<HTMLDivElement | null>;
  effortGroupRef: RefObject<HTMLDivElement | null>;
  eligible: boolean;
  hasWideActions: boolean;
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
    hasWideActions,
    contentKey,
  } = input;
  const [layout, setLayout] = useState({ visible: false, height: 0 });
  const visibleRef = useRef(false);
  const measure = useCallback(() => {
    const form = formRef.current;
    const band = bandRef.current;
    if (!host || !workspace || !form || !band) return;
    const groups = [accountGroupRef.current, effortGroupRef.current];
    const groupSizes = groups.map((group) => group?.getBoundingClientRect());
    const groupWidths = groupSizes.map((size) => size?.width ?? 0);
    const groupHeights = groupSizes.map((size) => size?.height ?? 0);
    const overlay = host.closest<HTMLElement>("[data-chat-composer-overlay]");
    if (!overlay) return;
    const currentBandHeight = band.getBoundingClientRect().height;
    const stack = form.closest<HTMLElement>("[data-chat-composer-stack]");
    const composerHeight =
      overlay.dataset.chatComposerLayout === "hero"
        ? (stack?.getBoundingClientRect().height ?? form.getBoundingClientRect().height) +
          (host.parentElement?.getBoundingClientRect().height ?? currentBandHeight)
        : overlay.getBoundingClientRect().height;
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
      composerHeight,
      currentBandHeight,
      groupWidths,
      groupHeights,
      hasWideActions,
      previousVisible: visibleRef.current,
    });
    const height = visible ? Math.max(0, ...groupHeights) : 0;
    visibleRef.current = visible;
    setLayout((previous) =>
      previous.visible === visible && previous.height === height ? previous : { visible, height },
    );
  }, [
    host,
    workspace,
    formRef,
    bandRef,
    accountGroupRef,
    effortGroupRef,
    eligible,
    hasWideActions,
  ]);

  useLayoutEffect(() => {
    measure();
    if (typeof ResizeObserver === "undefined") return;
    const observer = new ResizeObserver(measure);
    for (const element of [
      host,
      workspace,
      formRef.current,
      bandRef.current,
      accountGroupRef.current,
      effortGroupRef.current,
      host?.closest<HTMLElement>("[data-chat-composer-overlay]"),
    ]) {
      if (element) observer.observe(element);
    }
    document.fonts?.addEventListener("loadingdone", measure);
    return () => {
      observer.disconnect();
      document.fonts?.removeEventListener("loadingdone", measure);
    };
  }, [host, workspace, formRef, bandRef, accountGroupRef, effortGroupRef, contentKey, measure]);

  return { visible: eligible && layout.visible, height: eligible ? layout.height : 0 };
}

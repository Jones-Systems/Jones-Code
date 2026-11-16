import { useLayoutEffect, useRef, type ComponentProps, type RefCallback } from "react";
import { observeResize } from "../../lib/observeResize";
import { cn } from "../../lib/utils";
import styles from "./ComposerLayout.module.css";

export const composerLayoutPromptClassName = styles.prompt;
export const composerLayoutPlaceholderClassName = styles.placeholder;

/** The canvas owns the height budget; the composer and its portalled rails share it. */
export function ComposerLayout({
  hero,
  onAttach,
  className,
  ...props
}: Omit<ComponentProps<"div">, "ref"> & {
  hero: boolean;
  onAttach?: RefCallback<HTMLDivElement>;
}) {
  const elementRef = useRef<HTMLDivElement | null>(null);

  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- Subscribe to the independent canvas size, never the composer height that these presentation rules change.
  useLayoutEffect(() => {
    const element = elementRef.current;
    const canvas = element?.parentElement;
    if (!element || !canvas) return;
    const measure = () => {
      const height = canvas.clientHeight;
      const rem = Number.parseFloat(getComputedStyle(document.documentElement).fontSize);
      element.dataset.short = String(height <= 28 * rem);
      element.style.setProperty("--composer-pane-height", `${height}px`);
    };
    measure();
    const stop = observeResize(canvas, measure);
    window.addEventListener("resize", measure);
    return () => {
      stop();
      window.removeEventListener("resize", measure);
    };
  }, []);

  return (
    <div
      {...props}
      ref={(element) => {
        elementRef.current = element;
        return onAttach?.(element);
      }}
      data-chat-composer-overlay="true"
      data-chat-composer-layout={hero ? "hero" : "docked"}
      className={cn(styles.layout, className)}
    />
  );
}

export function ComposerLayoutLane({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      {...props}
      data-composer-layout-lane
      className={cn("chat-composer-lane w-full", className)}
    />
  );
}

export function ComposerLayoutHeadline({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      {...props}
      data-composer-layout-headline
      className={cn("absolute inset-x-0 bottom-full", className)}
    />
  );
}

export function ComposerLayoutBody({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      {...props}
      data-chat-composer-body="true"
      className={cn("relative px-4 pb-2 pt-3.5 sm:pt-4", className)}
    />
  );
}

export function ComposerLayoutActions({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      {...props}
      data-chat-composer-footer="true"
      className={cn(
        "flex min-w-0 flex-nowrap items-center overflow-visible px-2 pb-2",
        className,
      )}
    />
  );
}

export function ComposerLayoutFooter({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      {...props}
      data-composer-layout-footer
      className={cn("w-full ps-(--workspace-gutter-start) pe-(--workspace-gutter-end)", className)}
    />
  );
}

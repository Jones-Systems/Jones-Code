import { useLayoutEffect, useRef, type ReactNode } from "react";
import { observeResize } from "../../lib/observeResize";
import "./compactSidebarHeader.css";

export function CompactSidebarHeader({ children }: { children: ReactNode }) {
  const availableRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);

  useLayoutEffect(() => {
    const available = availableRef.current;
    const content = contentRef.current;
    if (!available || !content) return;
    const fit = () => {
      const setSpacing = (factor: number) => {
        available.style.setProperty("--sidebar-header-spacing", String(factor));
      };
      const width = () => content.getBoundingClientRect().width;
      const limit = available.clientWidth;
      available.dataset.density = "full";
      setSpacing(1);
      if (width() <= limit) return;

      available.dataset.density = "code-hidden";
      const originalWidth = width();
      if (originalWidth <= limit) return;

      // Measure both endpoints with J3 present; intrinsic count and label widths do not scale.
      setSpacing(0.25);
      const minimumWidth = width();
      if (minimumWidth > limit) {
        available.dataset.density = "minimal";
        return;
      }
      available.dataset.density = "continuous";
      const scalableWidth = originalWidth - minimumWidth;
      // Leave a fractional pixel for independently rounded flex gaps and number padding.
      setSpacing(
        Math.max(0.25, Math.min(1, 0.25 + (0.75 * (limit - minimumWidth - 0.25)) / scalableWidth)),
      );
    };
    fit();
    return observeResize([available, content], fit);
  }, []);

  return (
    <div
      ref={availableRef}
      data-sidebar-header
      className="relative z-10 min-w-0 flex-1"
      data-density="full"
    >
      <div ref={contentRef} data-sidebar-header-content>
        {children}
      </div>
    </div>
  );
}

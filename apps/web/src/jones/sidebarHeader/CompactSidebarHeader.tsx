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
      // Measure the real controls: counts, environment labels and Work state vary independently.
      for (const density of ["full", "code-hidden", "third", "half", "quarter", "minimal"]) {
        available.dataset.density = density;
        if (content.getBoundingClientRect().width <= available.clientWidth) break;
      }
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

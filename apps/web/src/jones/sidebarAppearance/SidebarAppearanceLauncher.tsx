import { SlidersHorizontal, XIcon } from "lucide-react";
import { useId, useLayoutEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { observeResize } from "~/lib/observeResize";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { SidebarAppearanceControls } from "./SidebarAppearanceControls";
import { useSidebarAppearanceHostContext } from "./useSidebarAppearanceHost";

function focusPanel(node: HTMLDivElement | null) {
  node?.focus();
}

export function SidebarAppearanceLauncher() {
  const host = useSidebarAppearanceHostContext();
  const [open, setOpen] = useState(false);
  const [position, setPosition] = useState<{ left: number; top: number; width: number } | null>(
    null,
  );
  const trigger = useRef<HTMLButtonElement>(null);
  const id = useId();
  const close = () => {
    host.setComparingPreset(null);
    setOpen(false);
    trigger.current?.focus();
  };
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- positions the dialog DOM at the measured sidebar edge and tracks sidebar/window resizing while open
  useLayoutEffect(() => {
    if (!open) return;
    const measure = () => {
      const anchor = trigger.current;
      if (!anchor) return;
      const sidebar =
        anchor.closest('[data-slot="sidebar"]') ?? anchor.closest('[data-sidebar="sidebar"]');
      const rect = (sidebar ?? anchor).getBoundingClientRect();
      const available = window.innerWidth - rect.right - 12;
      setPosition(
        available < 280
          ? null
          : {
              left: rect.right + 6,
              top: Math.max(8, Math.min(rect.top, window.innerHeight - 160)),
              width: Math.min(320, available),
            },
      );
    };
    measure();
    const anchor = trigger.current;
    const sidebar =
      anchor?.closest('[data-slot="sidebar"]') ?? anchor?.closest('[data-sidebar="sidebar"]');
    const stopObserving = anchor ? observeResize(sidebar ?? anchor, measure) : () => {};
    window.addEventListener("resize", measure);
    return () => {
      stopObserving();
      window.removeEventListener("resize", measure);
    };
  }, [open]);
  const content = open ? (
    <div
      ref={focusPanel}
      id={id}
      role="dialog"
      aria-modal={false}
      aria-label="Sidebar appearance"
      tabIndex={-1}
      className="z-20 max-h-[calc(100dvh-1rem)] overflow-y-auto rounded-lg border border-border bg-popover p-3 text-popover-foreground shadow-lg"
      style={
        position ? { position: "fixed", ...position } : { position: "relative", width: "100%" }
      }
      onKeyDown={(event) => {
        if (event.key === "Escape") {
          event.preventDefault();
          event.stopPropagation();
          close();
        }
      }}
    >
      <div className="mb-3 flex items-center justify-between gap-2">
        <h2 className="text-sm font-medium">Sidebar appearance</h2>
        <button
          type="button"
          aria-label="Close sidebar appearance"
          className="cursor-pointer"
          onClick={close}
        >
          <XIcon className="size-4" />
        </button>
      </div>
      <SidebarAppearanceControls />
    </div>
  ) : null;
  return (
    <>
      <div className="flex h-6 justify-end">
        <Tooltip>
          <TooltipTrigger
            render={
              <button
                ref={trigger}
                type="button"
                className="flex size-6 cursor-pointer items-center justify-center text-muted-foreground hover:text-foreground"
                aria-label="Sidebar appearance"
                aria-haspopup="dialog"
                aria-expanded={open}
                aria-controls={open ? id : undefined}
                onClick={() => (open ? close() : setOpen(true))}
              >
                <SlidersHorizontal className="size-3.5" />
              </button>
            }
          />
          <TooltipPopup>Sidebar appearance</TooltipPopup>
        </Tooltip>
      </div>
      {position && content ? createPortal(content, document.body) : content}
    </>
  );
}

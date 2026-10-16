import { ChevronRightIcon } from "lucide-react";
import type { ReactNode } from "react";
import { cn } from "../../lib/utils";

/** The quiet text toggle that reveals secondary detail, matching the row's Routes control. */
export function DisclosureButton({
  open,
  controls,
  onToggle,
  children,
}: {
  readonly open: boolean;
  readonly controls: string;
  readonly onToggle: () => void;
  readonly children: ReactNode;
}) {
  return (
    <button
      type="button"
      aria-expanded={open}
      aria-controls={controls}
      onClick={onToggle}
      className="inline-flex shrink-0 items-center gap-0.5 rounded-sm text-muted-foreground outline-none hover:text-foreground focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring"
    >
      {children}
      <ChevronRightIcon
        aria-hidden
        className={cn(
          "size-3 shrink-0 transition-transform duration-150 motion-reduce:transition-none",
          open && "rotate-90",
        )}
      />
    </button>
  );
}

/** Label/value pairs inside an opened disclosure. */
export function DetailList({ children }: { readonly children: ReactNode }) {
  return (
    <dl className="grid grid-cols-[auto_minmax(0,1fr)] items-baseline gap-x-3 gap-y-1">
      {children}
    </dl>
  );
}

export function DetailItem({
  label,
  children,
}: {
  readonly label: string;
  readonly children: ReactNode;
}) {
  return (
    <>
      <dt className="text-muted-foreground">{label}</dt>
      <dd className="flex min-w-0 flex-wrap items-baseline gap-x-1.5 gap-y-0.5">{children}</dd>
    </>
  );
}

export const DETAIL_PANEL_CLASSNAME =
  "space-y-2 rounded-md bg-muted/40 px-2.5 py-2 text-muted-foreground";

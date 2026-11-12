import { useId, type ReactNode } from "react";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";

export function UnavailableControl({
  reason,
  label,
  children,
}: {
  readonly reason: string | null;
  readonly label: string;
  readonly children: ReactNode;
}) {
  const id = useId();
  if (!reason) return children;
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <span
            tabIndex={0}
            role="group"
            aria-label={label}
            aria-disabled="true"
            aria-describedby={id}
            className="inline-flex rounded-md focus-visible:outline-2 focus-visible:outline-ring"
          />
        }
      >
        {children}
      </TooltipTrigger>
      <TooltipPopup id={id}>{reason}</TooltipPopup>
    </Tooltip>
  );
}

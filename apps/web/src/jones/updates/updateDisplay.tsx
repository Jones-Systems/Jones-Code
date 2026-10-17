import { useCopyToClipboard } from "../../hooks/useCopyToClipboard";
import { cn } from "../../lib/utils";
import { toastManager } from "../../components/ui/toast";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { compactJonesVersion } from "./buildVersion";

function useCopyValue(label: string) {
  return useCopyToClipboard<{ value: string }>({
    target: label,
    onCopy: (copied) => {
      toastManager.add({ type: "success", title: `Copied ${label}`, description: copied.value });
    },
    onError: (error) => {
      toastManager.add({
        type: "error",
        title: `Could not copy ${label}`,
        description: error.message,
      });
    },
  }).copyToClipboard;
}

const COPY_BUTTON_CLASSNAME =
  "cursor-pointer rounded-sm text-left outline-none hover:underline focus-visible:ring-1 focus-visible:ring-inset focus-visible:ring-ring";

/**
 * A build as `0.45 · 5620`. The tooltip carries the exact version and source commit,
 * and a click copies the exact version.
 */
export function BuildVersion({
  label,
  version,
  sourceSha,
}: {
  /** Names the build for assistive technology and the copy toast, e.g. "installed version". */
  readonly label: string;
  readonly version: string;
  readonly sourceSha?: string | undefined;
}) {
  const copy = useCopyValue(label);
  const { core, run } = compactJonesVersion(version);
  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <button
            type="button"
            aria-label={`Copy ${label} ${version}`}
            className={cn(COPY_BUTTON_CLASSNAME, "whitespace-nowrap tabular-nums")}
            onClick={() => copy(version, { value: version })}
          />
        }
      >
        <span className="font-medium text-foreground">{core}</span>
        {run !== undefined ? <span className="text-muted-foreground"> · {run}</span> : null}
      </TooltipTrigger>
      <TooltipPopup side="top" variant="code">
        {version}
        {sourceSha !== undefined ? (
          <>
            <br />
            source {sourceSha}
          </>
        ) : null}
      </TooltipPopup>
    </Tooltip>
  );
}

/** An exact value, shown whole or shortened; a click copies the exact value. */
export function CopyableValue({
  label,
  value,
  display = value,
}: {
  readonly label: string;
  readonly value: string;
  readonly display?: string;
}) {
  const copy = useCopyValue(label);
  const button = {
    type: "button",
    "aria-label": `Copy ${label} ${value}`,
    className: cn(COPY_BUTTON_CLASSNAME, "font-mono wrap-anywhere text-foreground"),
    onClick: () => copy(value, { value }),
  } as const;
  if (display === value) return <button {...button}>{display}</button>;
  return (
    <Tooltip>
      <TooltipTrigger render={<button {...button} />}>{display}</TooltipTrigger>
      <TooltipPopup side="top" variant="code">
        {value}
      </TooltipPopup>
    </Tooltip>
  );
}

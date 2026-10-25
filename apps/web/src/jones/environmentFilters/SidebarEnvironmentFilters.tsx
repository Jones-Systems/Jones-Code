import { useAtom } from "@effect/atom-react";
import { useLayoutEffect, useRef, useState } from "react";
import { APP_BUILD_STARTED_AT, APP_SOURCE_SHA, APP_VERSION } from "../../branding";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import { isElectron } from "../../env";
import { observeResize } from "../../lib/observeResize";
import { cn } from "../../lib/utils";
import { jonesBuildLabel } from "../updates/buildVersion";
import { SIDEBAR_ENVIRONMENT_FILTERS, toggleSidebarEnvironmentFilter } from "./model";
import { sidebarEnvironmentFiltersAtom } from "./state";

// The running client's own build, not the selected environment's: the same identity the update
// banner labels "Client". All inputs are fixed at build time, so the text is computed once.
const BUILD_LABEL = jonesBuildLabel({
  version: APP_VERSION,
  startedAt: APP_BUILD_STARTED_AT,
  sourceSha: APP_SOURCE_SHA,
});
const BUILD_DETAIL = `${BUILD_LABEL.detail} · ${isElectron ? "Desktop app" : "Web client"} ${APP_VERSION}${
  APP_SOURCE_SHA === undefined ? "" : ` · source ${APP_SOURCE_SHA}`
}`;

export function SidebarEnvironmentFilters() {
  const [filters, setFilters] = useAtom(sidebarEnvironmentFiltersAtom);
  const slotRef = useRef<HTMLDivElement>(null);
  const measureRef = useRef<HTMLSpanElement>(null);
  const compactMeasureRef = useRef<HTMLSpanElement>(null);
  const compactLabel = BUILD_LABEL.timed ? BUILD_LABEL.text.slice(-4) : BUILD_LABEL.text;
  const [label, setLabel] = useState({
    text: BUILD_LABEL.text,
    fontSize: undefined as number | undefined,
  });

  useLayoutEffect(() => {
    const slot = slotRef.current;
    const measure = measureRef.current;
    const compactMeasure = compactMeasureRef.current;
    if (!slot || !measure || !compactMeasure) return;
    const fit = () => {
      const available = slot.getBoundingClientRect().width;
      const maximum = Number.parseFloat(getComputedStyle(measure).fontSize);
      const fullSize = Math.min(
        maximum,
        (maximum * available) / measure.getBoundingClientRect().width,
      );
      const text = fullSize >= 6 ? BUILD_LABEL.text : compactLabel;
      const fontSize = Math.max(
        6,
        text === BUILD_LABEL.text
          ? fullSize
          : Math.min(maximum, (maximum * available) / compactMeasure.getBoundingClientRect().width),
      );
      setLabel((previous) =>
        previous.text === text && previous.fontSize === fontSize ? previous : { text, fontSize },
      );
    };
    fit();
    return observeResize([slot, measure, compactMeasure], fit);
  }, [compactLabel]);

  return (
    <div className="flex items-center gap-0.5 px-2 py-1">
      <div
        role="group"
        aria-label="Thread environments"
        className="flex shrink-0 items-center gap-2"
      >
        {SIDEBAR_ENVIRONMENT_FILTERS.map((label) => (
          <button
            key={label}
            type="button"
            aria-label={`Show ${label} threads`}
            aria-pressed={filters[label]}
            onClick={() => setFilters((current) => toggleSidebarEnvironmentFilter(current, label))}
            className={cn(
              "relative z-10 inline-flex h-6 shrink-0 items-center rounded-full px-2 text-xs font-medium outline-hidden ring-ring transition-colors focus-visible:ring-2 focus-visible:ring-inset [-webkit-app-region:no-drag]",
              filters[label]
                ? "bg-success/20 text-success-foreground ring-1 ring-success/30"
                : "bg-muted/30 text-muted-foreground hover:bg-muted/60",
            )}
          >
            {label}
          </button>
        ))}
      </div>
      <div ref={slotRef} className="relative min-w-0 flex-1 overflow-hidden">
        <span
          ref={measureRef}
          aria-hidden="true"
          className="pointer-events-none invisible absolute whitespace-nowrap text-2xs tabular-nums"
        >
          {BUILD_LABEL.text}
        </span>
        <span
          ref={compactMeasureRef}
          aria-hidden="true"
          className="pointer-events-none invisible absolute whitespace-nowrap text-2xs tabular-nums"
        >
          {compactLabel}
        </span>
        <Tooltip>
          <TooltipTrigger
            render={
              <span
                role="note"
                tabIndex={0}
                aria-label={BUILD_DETAIL}
                className="ms-auto block w-fit max-w-full select-none whitespace-nowrap rounded-sm text-2xs tabular-nums text-muted-foreground/60 outline-hidden ring-ring focus-visible:ring-2 focus-visible:ring-inset [-webkit-app-region:no-drag]"
                style={{ fontSize: label.fontSize }}
              >
                {label.text}
              </span>
            }
          />
          <TooltipPopup side="bottom">{BUILD_DETAIL}</TooltipPopup>
        </Tooltip>
      </div>
    </div>
  );
}

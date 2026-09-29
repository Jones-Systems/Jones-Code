import type { HostStatusSnapshot } from "@t3tools/contracts";
import { memo, useEffect, useState } from "react";

import {
  HOST_STATUS_IDS,
  HOST_STATUS_NAMES,
  hostStatusMetrics,
  observeHostStatus,
} from "../../hostStatus";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";

const healthClasses = {
  healthy: "border-emerald-500/60 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
  warning: "border-amber-500/70 bg-amber-500/15 text-amber-800 dark:text-amber-200",
  critical: "border-red-500/70 bg-red-500/15 text-red-700 dark:text-red-300",
  unavailable: "border-muted-foreground/30 bg-muted text-muted-foreground",
};

export const HostStatusIndicators = memo(function HostStatusIndicators() {
  const [snapshot, setSnapshot] = useState<HostStatusSnapshot | null>(null);
  useEffect(() => observeHostStatus(document, setSnapshot), []);
  return (
    <div
      role="group"
      aria-label="Host status"
      className="[-webkit-app-region:no-drag] flex min-w-0 max-w-full shrink items-center gap-2 overflow-x-auto py-1"
    >
      {HOST_STATUS_IDS.map((id) => {
        const host = snapshot?.hosts.find((entry) => entry.id === id);
        const metrics = hostStatusMetrics(host);
        const detail = snapshot === null ? "Host status unavailable or loading" : metrics.detail;
        const name = HOST_STATUS_NAMES[id];
        return (
          <div
            key={id}
            role="group"
            aria-label={`${name} host status`}
            className="flex shrink-0 gap-0.5"
          >
            {[
              {
                id: "name",
                value: name,
                label: `${name}: ${detail}`,
                health:
                  host?.status === "available" ? ("healthy" as const) : ("unavailable" as const),
              },
              {
                id: "load",
                value: metrics.load,
                label: `${name} load: ${detail}`,
                health: metrics.loadHealth,
              },
              {
                id: "ram",
                value: metrics.ram,
                label: `${name} available RAM in GiB: ${detail}`,
                health: metrics.ramHealth,
              },
            ].map(({ id: metricId, value, label, health }) => (
              <Tooltip key={metricId}>
                <TooltipTrigger
                  render={
                    <span
                      role="img"
                      tabIndex={0}
                      aria-label={label}
                      className={`flex size-8 shrink-0 items-center justify-center rounded-full border text-3xs font-medium tabular-nums focus-visible:outline-2 focus-visible:outline-ring ${healthClasses[health]}`}
                    />
                  }
                >
                  {value}
                </TooltipTrigger>
                <TooltipPopup>{label}</TooltipPopup>
              </Tooltip>
            ))}
          </div>
        );
      })}
    </div>
  );
});

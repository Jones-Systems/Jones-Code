import type { EnvironmentId, HostStatusSnapshot } from "@t3tools/contracts";
import { memo, useEffect, useLayoutEffect, useRef, useState } from "react";

import {
  type HostStatusCache,
  HOST_STATUS_IDS,
  HOST_STATUS_NAMES,
  fetchHostStatus,
  hostStatusMetrics,
  observeHostStatus,
} from "../../hostStatus";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../ui/tooltip";
import { observeResize } from "~/lib/observeResize";

const healthClasses = {
  healthy: "border-emerald-500/30 bg-emerald-500/5",
  warning: "border-yellow-500/35 bg-yellow-500/8",
  elevated: "border-orange-500/35 bg-orange-500/8",
  critical: "border-red-500/35 bg-red-500/8",
  unavailable: "border-muted-foreground/15 bg-muted/50 text-muted-foreground/75",
};

const metricClasses = {
  healthy: "text-emerald-700 dark:text-emerald-300",
  warning: "text-yellow-800 dark:text-yellow-200",
  elevated: "text-orange-700 dark:text-orange-300",
  critical: "text-red-700 dark:text-red-300",
  unavailable: "text-muted-foreground",
};

const bubbleClasses =
  "inline-flex h-8 shrink-0 items-center justify-center whitespace-nowrap rounded-full border px-2 text-xs font-medium tabular-nums";

let retained: { environmentId: EnvironmentId; cache: HostStatusCache } | null = null;

export function clearRetainedHostStatus(environmentId: EnvironmentId) {
  if (retained?.environmentId === environmentId) retained = null;
}

export const HostStatusIndicators = memo(function HostStatusIndicators({
  environmentId,
}: {
  environmentId: EnvironmentId;
}) {
  return <EnvironmentHostStatusIndicators key={environmentId} environmentId={environmentId} />;
});

function EnvironmentHostStatusIndicators({ environmentId }: { environmentId: EnvironmentId }) {
  const [cache] = useState(() => {
    if (retained?.environmentId !== environmentId) {
      retained = { environmentId, cache: { snapshot: null, settledAt: -Infinity } };
    }
    return retained.cache;
  });
  const [snapshot, setSnapshot] = useState<HostStatusSnapshot | null>(() => cache.snapshot);
  const [visibleCount, setVisibleCount] = useState(0);
  const containerRef = useRef<HTMLDivElement>(null);
  const measurementRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    let active = true;
    const stop = observeHostStatus(
      document,
      (snapshot) => {
        if (active) setSnapshot(snapshot);
      },
      (signal) => fetchHostStatus(environmentId, signal),
      cache,
    );
    return () => {
      active = false;
      stop();
    };
  }, [environmentId, cache]);
  useLayoutEffect(() => {
    const container = containerRef.current;
    const measurement = measurementRef.current;
    if (!container || !measurement) return;
    const update = () => {
      const availableWidth = container.getBoundingClientRect().width;
      const bubbles = Array.from(measurement.children);
      const start = bubbles[0]?.getBoundingClientRect().left ?? 0;
      let count = 0;
      for (const bubble of bubbles) {
        const rect = bubble.getBoundingClientRect();
        if (rect.width === 0 || rect.right - start > availableWidth) break;
        count += 1;
      }
      setVisibleCount(count);
    };
    update();
    return observeResize([container, measurement, ...measurement.children], update);
  }, []);
  const hosts = HOST_STATUS_IDS.map((id) => {
    const host = snapshot?.hosts.find((entry) => entry.id === id);
    const metrics = hostStatusMetrics(host);
    const detail = snapshot === null ? "Host status unavailable or loading" : metrics.detail;
    const name = HOST_STATUS_NAMES[id];
    return {
      id,
      metrics,
      label: `${name}: ${detail}`,
      text: (
        <>
          {name} · <span className={metricClasses[metrics.cpuHealth]}>{metrics.cpu}</span> ·{" "}
          <span className={metricClasses[metrics.ramHealth]}>{metrics.ram}</span>
        </>
      ),
    };
  });
  return (
    <div
      ref={containerRef}
      role="group"
      aria-label="Host status"
      className="[-webkit-app-region:no-drag] relative flex min-w-0 flex-1 items-center gap-2 overflow-clip py-1"
    >
      {/* Inert copies retain each host's live width even while its control is absent.
          The flex basis stays zero, so showing bubbles cannot reduce the title's allocation. */}
      <div
        ref={measurementRef}
        aria-hidden="true"
        data-host-status-measurement
        className="pointer-events-none invisible absolute left-0 top-0 flex w-max items-center gap-2"
      >
        {hosts.map(({ id, metrics, text }) => (
          <span key={id} className={`${bubbleClasses} ${healthClasses[metrics.health]}`}>
            {text}
          </span>
        ))}
      </div>
      {hosts.slice(0, visibleCount).map(({ id, metrics, label, text }) => (
        <Tooltip key={id}>
          <TooltipTrigger
            render={
              <span
                role="img"
                tabIndex={0}
                aria-label={label}
                className={`${bubbleClasses} focus-visible:outline-2 focus-visible:outline-ring ${healthClasses[metrics.health]}`}
              />
            }
          >
            {text}
          </TooltipTrigger>
          <TooltipPopup>{label}</TooltipPopup>
        </Tooltip>
      ))}
    </div>
  );
}

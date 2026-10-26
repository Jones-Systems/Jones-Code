import styles from "./ResponsiveHostStatusPills.module.css";
import { useLayoutEffect, useRef, useState, type CSSProperties } from "react";
import type { hostStatusMetrics } from "../../hostStatus";
import { observeResize } from "../../lib/observeResize";
import { Tooltip, TooltipPopup, TooltipTrigger } from "../../components/ui/tooltip";
import {
  fitHostStatus,
  HOST_DISPLAY_STAGES,
  hostDisplayValues,
  type HostLayoutFit,
} from "./responsiveLayout";

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
export interface HostStatusDisplay {
  id: string;
  name: string;
  label: string;
  metrics: Pick<
    ReturnType<typeof hostStatusMetrics>,
    "cpu" | "ram" | "health" | "cpuHealth" | "ramHealth"
  >;
}

export function ResponsiveHostStatusPills({ hosts }: { hosts: HostStatusDisplay[] }) {
  const containerRef = useRef<HTMLDivElement>(null);
  const measurementRef = useRef<HTMLDivElement>(null);
  const [fit, setFit] = useState<(HostLayoutFit & { cells: number[]; fontSize: number }) | null>(
    null,
  );
  const labels = [...new Set(HOST_DISPLAY_STAGES.flatMap((stage) => stage.names).filter(Boolean))];
  // oxlint-disable-next-line t3code/react-effect-budget -- Fit live text and own width/font observation; the zero-basis slot cannot steal the breadcrumb's allocation.
  useLayoutEffect(() => {
    const container = containerRef.current;
    const measurement = measurementRef.current;
    if (!container || !measurement) return;
    const update = () => {
      const widths = new Map(
        Array.from(measurement.children, (element) => [
          element.getAttribute("data-measure")!,
          element.getBoundingClientRect().width,
        ]),
      );
      const read = (key: string) => widths.get(key) ?? 0;
      const measured = hosts.map((host) => ({
        name: (name: string, inline: boolean) => read(`name:${name}:${inline}`),
        cpu: (unit: boolean) => read(`${host.id}:cpu:${unit}`),
        ram: (unit: boolean) => read(`${host.id}:ram:${unit}`),
        dot: read("dot"),
      }));
      const next = fitHostStatus(container.getBoundingClientRect().width, measured);
      const cells = measured.map((host) => Math.max(host.cpu(false), host.ram(false)));
      const fontSize = Number.parseFloat(getComputedStyle(measurement).fontSize);
      setFit((previous) => {
        if (next === null) return previous === null ? previous : null;
        if (
          previous?.stage === next.stage &&
          previous.padding === next.padding &&
          previous.gap === next.gap &&
          previous.scale === next.scale &&
          previous.fontSize === fontSize &&
          cells.every((cell, index) => cell === previous.cells[index])
        )
          return previous;
        return { ...next, cells, fontSize };
      });
    };
    update();
    return observeResize([container, measurement], update);
  }, [hosts]);
  const stage = fit ? HOST_DISPLAY_STAGES[fit.stage]! : null;
  return (
    <div
      ref={containerRef}
      role="group"
      aria-label="Host status"
      className="[-webkit-app-region:no-drag] relative flex min-w-0 flex-1 items-center overflow-clip py-1"
    >
      <div
        ref={measurementRef}
        aria-hidden="true"
        data-host-status-measurement
        className="pointer-events-none invisible absolute left-0 top-0 flex w-max whitespace-nowrap text-xs font-medium tabular-nums"
      >
        {labels.flatMap((name) =>
          [false, true].map((inline) => (
            <span key={`name:${name}:${inline}`} data-measure={`name:${name}:${inline}`}>
              {name}
              {inline ? " · " : ""}
            </span>
          )),
        )}
        <span data-measure="dot"> · </span>
        {hosts.flatMap((host) =>
          [false, true].flatMap((unit) => [
            <span key={`${host.id}:cpu:${unit}`} data-measure={`${host.id}:cpu:${unit}`}>
              {unit ? host.metrics.cpu : host.metrics.cpu.replace(/%$/, "")}
            </span>,
            <span key={`${host.id}:ram:${unit}`} data-measure={`${host.id}:ram:${unit}`}>
              {unit ? host.metrics.ram : host.metrics.ram.replace(/ mb\/s$/, "")}
            </span>,
          ]),
        )}
      </div>
      {fit && stage ? (
        <div
          className={styles["host-status-pills"]}
          data-host-status-stage={fit.stage}
          style={
            {
              "--host-gap": `${fit.gap}px`,
              "--host-padding": `${fit.padding}px`,
              "--host-font-size": `${fit.fontSize * fit.scale}px`,
            } as CSSProperties
          }
        >
          {hosts.map(({ id, metrics, label }, index) => {
            const layout = stage.layouts[index]!;
            const name = stage.names[index]!;
            const values = hostDisplayValues(metrics.cpu, metrics.ram, stage, index);
            return (
              <Tooltip key={id}>
                <TooltipTrigger
                  render={
                    <span
                      role="img"
                      tabIndex={0}
                      aria-label={label}
                      data-host-id={id}
                      data-pair={layout}
                      style={
                        {
                          "--host-cell-width": `${fit.cells[index]! * fit.scale}px`,
                        } as CSSProperties
                      }
                      className={`${styles["host-status-pill"]} whitespace-nowrap rounded-full border text-xs font-medium tabular-nums focus-visible:outline-2 focus-visible:outline-ring ${healthClasses[metrics.health]}`}
                    />
                  }
                >
                  {name ? (
                    <span data-host-name>
                      {name}
                      {layout === "inline" ? " · " : ""}
                    </span>
                  ) : null}
                  <span className={styles["host-status-pair"]}>
                    <span
                      data-host-value
                      data-host-cpu
                      className={`${styles["host-status-value"]} ${styles["host-status-cpu"]} ${metricClasses[metrics.cpuHealth]}`}
                    >
                      {values.cpu}
                    </span>
                    {layout === "inline" ? (
                      " · "
                    ) : layout === "diagonal" ? (
                      <svg
                        aria-hidden="true"
                        className={`${styles["host-status-slash"]} text-muted-foreground`}
                        viewBox="0 0 7 8"
                      >
                        <path d="M1 7L6 1" />
                      </svg>
                    ) : null}
                    <span
                      data-host-value
                      data-host-ram
                      className={`${styles["host-status-value"]} ${styles["host-status-ram"]} ${metricClasses[metrics.ramHealth]}`}
                    >
                      {values.ram}
                    </span>
                  </span>
                </TooltipTrigger>
                <TooltipPopup>{label}</TooltipPopup>
              </Tooltip>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

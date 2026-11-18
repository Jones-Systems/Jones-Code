import { CheckIcon, CircleIcon, MinusIcon } from "lucide-react";
import { cn } from "../../lib/utils";
import type { PromptStep } from "./promptPresentation";

const statusLabels: Readonly<Record<PromptStep["status"], string>> = {
  observed: "Observed",
  current: "Current",
  not_observed: "Not observed",
  not_applicable: "Not applicable",
};

/**
 * The fixed progress rail. Each step states only what was observed; the final line is always
 * "Finished: not tracked" because the queue does not report completed work.
 */
export function PromptProgress({
  steps,
  label,
  formatTime,
}: {
  readonly steps: readonly PromptStep[];
  readonly label: string;
  readonly formatTime: (ms: number) => string;
}) {
  const current = steps.findLastIndex(
    (step) => step.status === "observed" || step.status === "current",
  );
  return (
    <ol className="m-0 list-none space-y-1 p-0 text-xs" aria-label={label}>
      {steps.map((step, index) => (
        <li
          key={step.key}
          data-step={step.key}
          data-step-status={step.status}
          aria-current={index === current ? "step" : undefined}
          className={cn(
            "flex min-w-0 items-start gap-2",
            (step.status === "not_observed" || step.status === "not_applicable") &&
              "text-muted-foreground",
          )}
        >
          <span aria-label={statusLabels[step.status]} className="mt-0.5 flex size-3 shrink-0">
            {step.status === "observed" ? (
              <CheckIcon className="size-3 text-success" />
            ) : step.status === "current" ? (
              <span className="m-auto size-2 rounded-full bg-primary" />
            ) : step.status === "not_applicable" ? (
              <MinusIcon className="size-3" />
            ) : (
              <CircleIcon className="size-3" />
            )}
          </span>
          <span className="min-w-0 flex-1 wrap-anywhere">
            <span className="font-medium">{step.label}</span>
            {": "}
            {step.detail}
          </span>
          {step.time !== null ? (
            <time className="shrink-0 tabular-nums" dateTime={new Date(step.time).toISOString()}>
              {formatTime(step.time)}
            </time>
          ) : null}
        </li>
      ))}
    </ol>
  );
}

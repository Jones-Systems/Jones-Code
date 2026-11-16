import type { InstallProgressView } from "@t3tools/client-runtime/jones/install-plan";
import { CheckIcon, CircleIcon, XIcon } from "lucide-react";
import { cn } from "../../lib/utils";
import { formatElapsed } from "./copy";
import { Elapsed } from "./Elapsed";

const STATUS_LABEL = {
  done: "Done",
  current: "Running",
  pending: "Waiting",
  failed: "Failed",
} as const;

/**
 * The host's steps for one download or install: done with its duration, the current step with a
 * live timer, pending steps, and a failed step with its error.
 */
export function ProgressSteps({ progress }: { readonly progress: InstallProgressView }) {
  if (progress.steps.length === 0)
    return (
      <>
        <p data-progress-step={progress.stepKey}>
          Step {progress.stepIndex} of {progress.stepCount} · {progress.stepLabel}
          {progress.active ? (
            <>
              {" · "}
              <Elapsed since={progress.stepStartedAt} skewMs={progress.skewMs} />
            </>
          ) : null}
        </p>
        {progress.phase === "failed" && progress.error !== undefined ? (
          <p className="text-destructive" role="status">
            {progress.error.message}
          </p>
        ) : null}
      </>
    );
  return (
    <ol className="m-0 list-none space-y-0.5 p-0" aria-label="Update steps" data-progress-steps>
      {progress.steps.map((step) => (
        <li
          key={step.key}
          data-step-status={step.status}
          className={cn(
            "flex min-w-0 items-start gap-1.5",
            step.status === "pending" && "opacity-60",
            step.status === "current" && "text-foreground",
          )}
        >
          <span aria-label={STATUS_LABEL[step.status]} className="mt-0.5 flex size-3 shrink-0">
            {step.status === "done" ? (
              <CheckIcon className="size-3 text-success" />
            ) : step.status === "failed" ? (
              <XIcon className="size-3 text-destructive" />
            ) : step.status === "current" ? (
              <span className="m-auto size-2 rounded-full bg-primary" />
            ) : (
              <CircleIcon className="size-3" />
            )}
          </span>
          <span className="min-w-0 flex-1">
            <span className="block truncate">{step.label}</span>
            {step.status === "failed" && progress.error !== undefined ? (
              <span className="block text-destructive" role="status">
                {progress.error.message}
              </span>
            ) : null}
          </span>
          <span className="shrink-0 tabular-nums">
            {step.status === "current" && progress.active ? (
              <Elapsed since={progress.stepStartedAt} skewMs={progress.skewMs} />
            ) : step.durationMs !== undefined ? (
              formatElapsed(step.durationMs)
            ) : null}
          </span>
        </li>
      ))}
    </ol>
  );
}

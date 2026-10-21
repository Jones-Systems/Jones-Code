import { formatPercent, formatTokens, formatUsd } from "@t3tools/shared/usageFormat";
import type { MergedUsage } from "@t3tools/shared/usageMerge";

import { cn } from "../../lib/utils";

export function formatCachePercent(cachedInputTokens: number, totalTokens: number): string {
  return totalTokens > 0 ? formatPercent(cachedInputTokens / totalTokens) : "—";
}

export function formatCacheWrite(tokens: number): string {
  return tokens > 0 ? formatTokens(tokens) : "0 reported";
}

export function UsageTotals({
  usage,
  loading,
}: {
  readonly usage: MergedUsage;
  readonly loading: boolean;
}) {
  const metrics = [
    ["Processed tokens", formatTokens(usage.totalTokens)],
    ["Cached input", formatTokens(usage.cachedInputTokens)],
    ["Uncached input", formatTokens(usage.uncachedInputTokens)],
    ["Cache write", formatCacheWrite(usage.cacheCreationTokens)],
    ["Output", formatTokens(usage.outputTokens)],
    ["Cache savings", formatUsd(usage.costQuality.cacheSavingsUsd)],
    ["Cache %", formatCachePercent(usage.cachedInputTokens, usage.totalTokens)],
  ];
  return (
    <dl className="flex flex-wrap justify-between gap-x-6 gap-y-4 py-1">
      {metrics.map(([label, value]) => (
        <div key={label} className="flex shrink-0 flex-col gap-0.5 whitespace-nowrap">
          <dt className="text-xs text-muted-foreground">{label}</dt>
          <dd
            className={cn(
              "text-base font-medium text-foreground tabular-nums transition-opacity",
              loading && "opacity-40 delay-150",
            )}
          >
            {value}
          </dd>
        </div>
      ))}
    </dl>
  );
}

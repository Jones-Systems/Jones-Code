import { formatTokens, formatUsd } from "@t3tools/shared/usageFormat";
import { isModelCostUnknown, type ModelTotals } from "@t3tools/shared/usageMerge";

import { costPerMillionTokens } from "../../components/usage/usageBreakdown";
import { formatCachePercent, formatCacheWrite } from "./UsageTotals";

export function UsageModelTokenStats({ model }: { readonly model: ModelTotals }) {
  const perMillion = costPerMillionTokens(model);
  const stats = [
    { label: "Cost", value: isModelCostUnknown(model) ? "Unpriced" : formatUsd(model.costUsd) },
    { label: "Tokens", value: formatTokens(model.totalTokens) },
    perMillion === null ? null : { label: "Per 1M tokens", value: formatUsd(perMillion) },
    {
      label: "Cache %",
      value: formatCachePercent(model.totals.cachedInputTokens, model.totalTokens),
    },
    { label: "Cache read", value: formatTokens(model.totals.cachedInputTokens) },
    { label: "Cache write", value: formatCacheWrite(model.totals.cacheCreationTokens) },
  ].filter((stat) => stat !== null);
  return (
    <div className="flex flex-col gap-3">
      <dl className="flex flex-wrap justify-between gap-x-6 gap-y-4">
        {stats.map((stat) => (
          <div key={stat.label} className="flex shrink-0 flex-col gap-0.5 whitespace-nowrap">
            <dt className="text-xs text-muted-foreground">{stat.label}</dt>
            <dd className="text-xl font-semibold text-foreground tabular-nums">{stat.value}</dd>
          </div>
        ))}
      </dl>
      <p className="text-xs text-muted-foreground">
        Recorded counters may omit token breakdowns. Zero does not establish complete coverage.
      </p>
    </div>
  );
}

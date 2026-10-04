import type { UsageProviderKind } from "@t3tools/contracts";
import type {
  DailyTotals,
  HourlyTotals,
  MergedUsage,
  ModelTotals,
} from "@t3tools/shared/usageMerge";

export function sortModelsByTokens(models: readonly ModelTotals[]) {
  return models.toSorted(
    (left, right) => right.totalTokens - left.totalTokens || right.costUsd - left.costUsd,
  );
}

function providerPeriods<T extends DailyTotals | HourlyTotals>(
  periods: readonly T[],
  provider: UsageProviderKind,
): readonly T[] {
  return periods.flatMap((period) => {
    const totals = period.byProvider.get(provider);
    return totals === undefined
      ? []
      : [{ ...period, ...totals, byProvider: new Map([[provider, totals]]) }];
  });
}

/** Projects accepted merged totals; provider focus never reclaims or remerges sources. */
export function selectUsageBreakdown(merged: MergedUsage, provider: UsageProviderKind | null) {
  const providerTotals = merged.providers.find((totals) => totals.provider === provider) ?? null;
  if (providerTotals === null) {
    return {
      providerTotals,
      models: merged.models,
      daily: merged.daily,
      hourly: merged.hourly,
    };
  }
  return {
    providerTotals,
    models: merged.models
      .filter((model) => model.provider === providerTotals.provider)
      .map((model) => ({
        ...model,
        costShare: providerTotals.costUsd === 0 ? 0 : model.costUsd / providerTotals.costUsd,
      })),
    daily: providerPeriods(merged.daily, providerTotals.provider),
    hourly: providerPeriods(merged.hourly, providerTotals.provider),
  };
}
